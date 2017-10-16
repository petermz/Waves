package com.wavesplatform.mining

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.wavesplatform.network._
import com.wavesplatform.settings.WavesSettings
import com.wavesplatform.state2.ByteStr
import com.wavesplatform.state2.reader.StateReader
import com.wavesplatform.{Coordinator, UtxPool}
import io.netty.channel.group.ChannelGroup
import kamon.Kamon
import kamon.metric.instrument
import monix.eval.Task
import monix.execution._
import monix.execution.cancelables.{CompositeCancelable, SerialCancelable}
import scorex.account.PrivateKeyAccount
import scorex.block.Block
import scorex.consensus.nxt.NxtLikeConsensusBlockData
import scorex.transaction.PoSCalc._
import scorex.transaction._
import scorex.utils.{ScorexLogging, Time}
import scorex.wallet.Wallet

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.math.Ordering.Implicits._

class Miner(
               allChannels: ChannelGroup,
               blockchainReadiness: AtomicBoolean,
               blockchainUpdater: BlockchainUpdater,
               checkpoint: CheckpointService,
               history: History,
               stateReader: StateReader,
               settings: WavesSettings,
               timeService: Time,
               utx: UtxPool,
               wallet: Wallet) extends ScorexLogging {

  import Miner._

  private implicit val scheduler = Scheduler.fixedPool(name = "miner-pool", poolSize = 2)

  private val minerSettings = settings.minerSettings
  private val blockchainSettings = settings.blockchainSettings
  private lazy val processBlock = Coordinator.processBlock(checkpoint, history, blockchainUpdater, timeService, stateReader, utx, blockchainReadiness, Miner.this, settings) _

  private val scheduledAttempts = SerialCancelable()

  private val blockBuildTimeStats = Kamon.metrics.histogram("block-build-time", instrument.Time.Milliseconds)

  private def checkAge(parentHeight: Int, parent: Block): Either[String, Unit] =
    Either
      .cond(parentHeight == 1, (), Duration.between(Instant.ofEpochMilli(parent.timestamp), Instant.ofEpochMilli(timeService.correctedTime())))
      .left.flatMap(blockAge => Either.cond(blockAge <= minerSettings.intervalAfterLastBlockThenGenerationIsAllowed, (),
      s"BlockChain is too old (last block ${parent.uniqueId} generated $blockAge ago)"
    ))

  private def generateOneBlockTask(account: PrivateKeyAccount, parentHeight: Int, parent: Block,
                                   greatGrandParent: Option[Block], balance: Long, unconfirmed: Seq[Transaction])(delay: FiniteDuration): Task[Either[String, Block]] = Task {
    val pc = allChannels.size()
    lazy val lastBlockKernelData = parent.consensusData
    val currentTime = timeService.correctedTime()
    val start = System.currentTimeMillis()
    log.debug(s"$start: Corrected time: $currentTime")
    lazy val h = calcHit(lastBlockKernelData, account)
    lazy val t = calcTarget(parent, currentTime, balance)
    for {
      _ <- Either.cond(pc >= minerSettings.quorum, (), s"Quorum not available ($pc/${minerSettings.quorum}, not forging block with ${account.address}")
      _ <- Either.cond(h < t, (), s"${System.currentTimeMillis()}: Hit $h was NOT less than target $t, not forging block with ${account.address}")
      _ = log.debug(s"Forging with ${account.address}, H $h < T $t, balance $balance, prev block ${parent.uniqueId}")
      _ = log.debug(s"Previous block ID ${parent.uniqueId} at $parentHeight with target ${lastBlockKernelData.baseTarget}")
      avgBlockDelay = blockchainSettings.genesisSettings.averageBlockDelay
      btg = calcBaseTarget(avgBlockDelay, parentHeight, parent, greatGrandParent, currentTime)
      gs = calcGeneratorSignature(lastBlockKernelData, account)
      consensusData = NxtLikeConsensusBlockData(btg, gs)
      _ = log.debug(s"Adding ${unconfirmed.size} unconfirmed transaction(s) to new block")
      block = Block.buildAndSign(Version, currentTime, parent.uniqueId, consensusData, unconfirmed, account)
      _ = blockBuildTimeStats.record(System.currentTimeMillis() - start)
    } yield block
  }.delayExecution(delay)

  private def generateBlockTask(account: PrivateKeyAccount): Task[Unit] = {
    val height = history.height()
    val lastBlock = history.lastBlock.get
    val grandParent = history.parent(lastBlock, 2)
    (for {
      _ <- checkAge(height, lastBlock)
      ts <- nextBlockGenerationTime(height, stateReader, blockchainSettings.functionalitySettings, lastBlock, account)
    } yield ts) match {
      case Right(ts) =>
        val offset = calcOffset(timeService, ts)
        log.debug(s"Next attempt for acc=$account in $offset")
        val balance = generatingBalance(stateReader, blockchainSettings.functionalitySettings, account, height)
        generateOneBlockTask(account, height, lastBlock, grandParent, balance, utx.packUnconfirmed())(offset).flatMap {
          case Right(block) => Task.now {
            processBlock(block, true) match {
              case Left(err) => log.warn(err.toString)
              case Right(score) =>
                allChannels.broadcast(LocalScoreChanged(score))
                allChannels.broadcast(BlockForged(block))
            }
          }
          case Left(err) =>
            log.debug(s"No block generated because $err, retrying")
            generateBlockTask(account)
        }
      case Left(err) =>
        log.debug(s"Not scheduling block mining because $err")
        Task.unit
    }
  }

  private def generateBlockInForkTask(account: PrivateKeyAccount, fork: ForkedHistory): Task[Unit] = {///converge
    val height = fork.height()
    val lastBlock = fork.lastBlock.get
    val grandParent = fork.parent(lastBlock, 2)
    (for {
      _ <- checkAge(height, lastBlock)
      ts <- nextBlockGenerationTime(height, stateReader, blockchainSettings.functionalitySettings, lastBlock, account)
    } yield ts) match {
      case Right(ts) =>
        val offset = calcOffset(timeService, ts)
        log.debug(s"Next attempt for acc=$account in $offset")
        val balance = generatingBalance(stateReader, blockchainSettings.functionalitySettings, account, height)
        generateOneBlockTask(account, height, lastBlock, grandParent, balance, Seq())(offset).flatMap { r =>
          r match {
            case Right(block) =>
              fork.addBlock(block)
            case Left(err) =>
              log.debug(s"No block generated because $err, retrying")
          }
          generateBlockInForkTask(account, fork)
        }
      case Left(err) =>
        log.debug(s"Not scheduling block mining because $err")
        Task.unit
    }
  }

  private val forks = mutable.Map[ForkedHistory, Cancelable]()

  def addFork(height: Int) = {
    if (minerSettings.enableForks) {
      val fork = new ForkedHistory(history, height)
      wallet.privateKeyAccounts().headOption.map(generateBlockInForkTask(_, fork)).foreach { task =>
        forks += (fork -> task.runAsync)
      }
    }
  }

  def dropFork(fork: ForkedHistory) = {
    forks.remove(fork).foreach(_.cancel)
  }

  def lastBlockChanged(): Unit = if (settings.minerSettings.enable) {
    log.debug("Miner notified of new block, restarting all mining tasks")
    scheduledAttempts := CompositeCancelable.fromSet(
      wallet.privateKeyAccounts().map(generateBlockTask).map(_.runAsync).toSet)
  } else {
    log.debug("Miner is disabled, ignoring last block change")
  }

  def shutdown(): Unit = ()
}

object Miner extends ScorexLogging {

  val Version: Byte = 2
  val MinimalGenerationOffsetMillis: Long = 1001

  def calcOffset(timeService: Time, calculatedTimestamp: Long): FiniteDuration = {
    val calculatedGenerationTimestamp = (Math.ceil(calculatedTimestamp / 1000.0) * 1000).toLong
    log.debug(s"CalculatedTS $calculatedTimestamp: CalculatedGenerationTS: $calculatedGenerationTimestamp")
    val calculatedOffset = calculatedGenerationTimestamp - timeService.correctedTime()
    Math.max(MinimalGenerationOffsetMillis, calculatedOffset).millis
  }
}

class ForkedHistory(private val history: History, private val commonHeight: Int) extends History with ScorexLogging {
  /// when history is rolled back, drop all forks above the rollback point
  private val fork = ListBuffer[Block]()

  def height() = commonHeight + fork.size
  def blockBytes(height: Int) = if (height <= commonHeight) history.blockBytes(height) else fork.lift(height - commonHeight - 1).map(_.bytes)
  def heightOf(blockId: ByteStr) = {
    val i = fork.indexWhere(_.uniqueId == blockId)
    Option(i).filter(_ > -1).orElse(history.heightOf(blockId))
  }

  // these are never used
  def scoreOf(id: ByteStr) = ???
  def lastBlockIds(howMany: Int) = ???

  lazy val synchronizationToken = new ReentrantReadWriteLock()
  def close() = {}

  def addBlock(block: Block) = {
    log.debug(s">>> FORK $this <<< Adding fork block at height ${height()+1}")
    fork += block
  }

  println(s">>> FORK $this <<< Starting from height $commonHeight")
  history.lastBlocks(history.height() - commonHeight) foreach addBlock
}
