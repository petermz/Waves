package com.wavesplatform.it

import com.wavesplatform.it.api._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.Await.result
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future.traverse
import scala.concurrent.duration._

class WideStateGenerationSuite extends FreeSpec with IntegrationNodesInitializationAndStopping
  with Matchers with TransferSending with MultipleNodesApi {

  override lazy val nodes: Seq[Node] = docker.startNodes(
    NodeConfigs.newBuilder
      .overrideBase(_.quorum(3))
      .withDefault(3)
      .withSpecial(_.nonMiner)
      .build
  )

  private val requestsCount = 10000

  "Generate a lot of transactions and synchronise" in result(for {
    b <- traverse(nodes)(balanceForNode).map(_.toMap)
    lastTx <- {
      log.debug(
        s"""Balances:
           |${b.map { case (account, balance) => s"$account -> $balance" }.mkString("\n")}""".stripMargin)
      processRequests(generateTransfersToRandomAddresses(requestsCount / 2, b) ++ generateTransfersBetweenAccounts(requestsCount / 2, b))
    }

    _ <- traverse(nodes)(_.waitForTransaction(lastTx.get.id))
    height <- traverse(nodes)(_.height).map(_.max)
    _ <- withClue(s"waitForSameBlocksAt($height)") {
      Await.ready(waitForSameBlocksAt(nodes, 5.seconds, height), 10.minutes)
    }
    heights <- traverse(nodes)(_.height)
  } yield (), 10.minutes)

}
