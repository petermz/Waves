package com.wavesplatform.mining

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicBoolean

import com.wavesplatform.network._
import com.wavesplatform.settings.WavesSettings
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
import scorex.transaction.assets.TransferTransaction
import scorex.transaction.{BlockchainUpdater, CheckpointService, History, PoSCalc}
import scorex.utils.{ScorexLogging, Time}
import scorex.wallet.Wallet

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

  private val accounts = {
    val seed = scorex.utils.randomBytes(64)
    wallet.privateKeyAccounts().toStream #::: (Stream.iterate(0)(_ + 1) map { Wallet.generateNewAccount(seed, _) })
  }

  private val blockBuildTimeStats = Kamon.metrics.histogram("block-build-time", instrument.Time.Milliseconds)

  private def checkAge(parentHeight: Int, parent: Block): Either[String, Unit] =
    Either
      .cond(parentHeight == 1, (), Duration.between(Instant.ofEpochMilli(parent.timestamp), Instant.ofEpochMilli(timeService.correctedTime())))
      .left.flatMap(blockAge => Either.cond(blockAge <= minerSettings.intervalAfterLastBlockThenGenerationIsAllowed, (),
      s"BlockChain is too old (last block ${parent.uniqueId} generated $blockAge ago)"
    ))

  private def generateOneBlockTask(account: PrivateKeyAccount, parentHeight: Int, parent: Block,
                                   greatGrandParent: Option[Block], balance: Long)(delay: FiniteDuration): Task[Either[String, Block]] = Task {
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
      unconfirmed = utx.packUnconfirmed()
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
        generateOneBlockTask(account, height, lastBlock, grandParent, balance)(offset).flatMap {
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

  def lastBlockChanged(): Unit =
    if (settings.minerSettings.enable) {
      if (! settings.minerSettings.enableGrinding) {
        // regular node
        log.debug("Miner notified of new block, restarting all mining tasks")
        scheduledAttempts := CompositeCancelable.fromSet(
          wallet.privateKeyAccounts().map(generateBlockTask).map(_.runAsync).toSet)
      } else if (! isGrinding.get()) { // attacking node
        // find an account with sufficient balance
        accounts.find(
          generatingBalance(stateReader, blockchainSettings.functionalitySettings, _, history.height()) >= PoSCalc.MinimalEffectiveBalanceForGenerator
        ).map { account =>
          val ts = System.currentTimeMillis - 14000
          log.debug(s"### Starting grinding attack with account $account at time $ts")
          scheduledAttempts.cancel
          scheduledAttempts := CompositeCancelable(Task { generateBlock(account, ts) }.runAsync)
        }
      }
    } else {
      log.debug("Miner is disabled, ignoring last block change")
    }

  private val isGrinding = new AtomicBoolean(false)

  private def generateBlock(account: PrivateKeyAccount, timestamp: Long): Unit = {
    val height = history.height()
    val parent = history.lastBlock.get
    val balance = generatingBalance(stateReader, blockchainSettings.functionalitySettings, account, height)
    for {
      timedelta <- {
        val td = 0 until 15000 find { d => canForge(account, balance, parent.consensusData, parent.timestamp, timestamp + d) }
        isGrinding.set(td.nonEmpty)
        if (td.isEmpty) log.debug(s"### $account failed to forge initial block at time $timestamp with balance $balance")
        td
      }
      ts = timestamp + timedelta
      grandParent = history.parent(parent, 2)
      avgBlockDelay = blockchainSettings.genesisSettings.averageBlockDelay
      btg = calcBaseTarget(avgBlockDelay, height, parent, grandParent, ts)
      gs = calcGeneratorSignature(parent.consensusData, account)
      consensusData = NxtLikeConsensusBlockData(btg, gs)
      fee = 100000
      delay = 1000 // ms

      _ = log.debug(s"### $account forges at height $height with balance $balance and target ${parent.consensusData.baseTarget}")
      nextTimestamp = ts + delay
      nextForger <- accounts.find(canForge(_, balance-fee, consensusData, ts, nextTimestamp))
      tx <- TransferTransaction.create(None, account, nextForger, balance-fee, ts, None, fee, Array()).toOption
      block = Block.buildAndSign(Version, ts, parent.uniqueId, consensusData, Seq(tx), account)
      _ = Thread.sleep(delay)
    } yield processBlock(block, true) match {
      case Left(err) =>
        log.warn(s"### Error: ${err.toString}")
        isGrinding.set(false)
      case Right(score) =>
        log.debug(s"### Block ${block.uniqueId} forged, score $score")
        log.debug(s"### Next block: forger=$nextForger, time=$nextTimestamp")
        allChannels.broadcast(LocalScoreChanged(score))
        allChannels.broadcast(BlockForged(block))
        generateBlock(nextForger, nextTimestamp)
    }
  }

  def canForge(account: PrivateKeyAccount, balance: Long, prevConsensusData: NxtLikeConsensusBlockData, prevTimestamp: Long, timestamp: Long) = {
    val h = calcHit(prevConsensusData, account)
    val eta = (timestamp - prevTimestamp) / 1000
    val t = BigInt(prevConsensusData.baseTarget) * eta * balance
    h < t
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