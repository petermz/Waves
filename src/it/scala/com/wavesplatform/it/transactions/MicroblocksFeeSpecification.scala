package com.wavesplatform.it.transactions

import com.wavesplatform.it.api.NodeApi.Transaction
import com.wavesplatform.it.util._
import com.wavesplatform.it.{IntegrationSuiteWithThreeAddresses, Node}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future.traverse
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random


class MicroblocksFeeSpecification(override val allNodes: Seq[Node], override val notMiner: Node)
  extends IntegrationSuiteWithThreeAddresses {

  private def txRequestsGen(n: Int, fee: Long): Future[Seq[Transaction]] = Future.sequence {
    (1 to n).map { _ =>
      sender.transfer(sender.address, firstAddress, (1 + Random.nextInt(10)).waves, fee)
    }
  }

  private def minerAndFee(blockHeight: Int) = for {
    blockInfo <- sender.blockAt(blockHeight)
    blockFee = blockInfo.fee
    miningNode = allNodes.find(_.address == blockInfo.generator).get

  } yield ()

  test("microblockheightactivation  and fee") {
    val microblockActivationHeight = 21

    val f = for {
      height <- traverse(allNodes)(_.height).map(_.max)

      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight - 2))
      sentTxs <- txRequestsGen(400, 2.waves)
      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight + 1))

      initialBalances <- sender.debugStateAt(microblockActivationHeight - 2) //100%
      balancesBeforeActivation <- sender.debugStateAt(microblockActivationHeight - 1) //40%
      balancesOnActivation <- sender.debugStateAt(microblockActivationHeight) //40% + 60%
      balancesAfterActivation <- sender.debugStateAt(microblockActivationHeight + 1)




      //miningNodeBalance = miningNode.balance(blockInfo.generator)
      balances <- sender.debugStateAt(microblockActivationHeight - 1)
      //nodeBalance = balances(firstMiningNode.address)
      //      sentTxs <- txRequestsGen(1, 10.waves)
      //      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight))
      //      activationBlock <- sender.blockAt(microblockActivationHeight)
      //      activationBlockFee = activationBlock.fee
      //      firstMiningNode = allNodes.find(_.address == activationBlock.generator).get
      //      genBalanceFirstMiner <- firstMiningNode.balance(activationBlock.generator)
      //
      //      _ <- assertBalances(firstMiningNode.address, (balances(firstMiningNode.address) + activationBlockFee), (balances(firstMiningNode.address) + activationBlockFee))

      sentTxs <- txRequestsGen(1, 12.waves)
      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight))
      nextBlock <- sender.blockAt(microblockActivationHeight)
      nextBlockFee = nextBlock.fee
      nextMiningNode = allNodes.find(_.address == nextBlock.generator).get
      genBalanceSecondMiner <- nextMiningNode.balance(nextBlock.generator)
      _ <- assertBalances(nextMiningNode.address, (balances(nextMiningNode.address) + nextBlockFee * 0.4).toLong, (balances(nextMiningNode.address) + nextBlockFee * 0.4).toLong)

      sentTxs <- txRequestsGen(1, 20.waves)
      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight + 2))
      _ <- traverse(allNodes)(_.waitForHeight(microblockActivationHeight))
      activationBlock <- sender.blockAt(microblockActivationHeight)
      activationBlockFee = activationBlock.fee
      firstMiningNode = allNodes.find(_.address == activationBlock.generator).get
      genBalanceFirstMiner <- firstMiningNode.balance(activationBlock.generator)

      //if firstMiningNode.address.equals(nextMiningNode.address) {
      nodeBalance = balances(firstMiningNode.address) + activationBlockFee * 0.4 + nextBlockFee * 0.6
      //}
      //    else {
      //    _
      //    }
      _ <- assertBalances(firstMiningNode.address, (balances(firstMiningNode.address) + activationBlockFee * 0.4 + nextBlockFee * 0.6).toLong, (balances(firstMiningNode.address) + activationBlockFee * 0.4 + nextBlockFee * 0.6).toLong)

      balances1 <- sender.debugStateAt(microblockActivationHeight - 1)
      balances1 <- sender.debugStateAt(microblockActivationHeight)
      balances1 <- sender.debugStateAt(microblockActivationHeight + 1)
      balances1 <- sender.debugStateAt(microblockActivationHeight + 2)
    } yield succeed
    //    } yield {
    //      println(firstMiningNode.address)
    //      println(nextMiningNode.address)
    //      println(genBalanceFirstMiner.balance - balances(firstMiningNode.address))
    //      println(genBalanceSecondMiner.balance - balances(nextMiningNode.address))
    //      println(activationBlockFee)
    //      println(nextBlockFee)
    //      genBalanceFirstMiner.balance shouldBe balances(firstMiningNode.address) + activationBlockFee*0.4
    //      genBalanceSecondMiner.balance shouldBe balances(nextMiningNode.address) + nextBlockFee * 0.4 + activationBlockFee*0.6
    //    }

    Await.result(f, 2.minute)
  }


}
