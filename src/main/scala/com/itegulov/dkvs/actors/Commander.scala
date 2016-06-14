package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.util.Timeout
import com.itegulov.dkvs.structure.{Address, BallotNumber, BallotProposal}

import scala.collection.mutable
import scala.language.postfixOps

/**
  * @author Daniyar Itegulov
  */
class Commander(ballotProposal: BallotProposal,
                acceptorsAddresses: Seq[Address],
                leader: ActorRef,
                resender: ActorRef)(implicit val timeout: Timeout) extends Actor with ActorLogging {
  private val waitFor = mutable.Set(acceptorsAddresses.indices: _*)

  val acceptors = acceptorsAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Acceptors@${address.hostname}:${address.port}/user/Acceptor$i")
  }

  override def preStart(): Unit = {
    log.info("New commander has been created with " +
      s"($ballotProposal, $acceptorsAddresses, $leader) arguments")
    acceptors.foreach(_ ! ("p2a", ballotProposal))
  }

  override def receive: Receive = {
    case ("p2b", acceptorId: Int, ballot: BallotNumber) =>
      log.info(s"New p2b response with ($acceptorId, $ballot) arguments")
      if (ballotProposal.ballot == ballot) {
        waitFor -= acceptorId
        log.info(s"${waitFor.size} acceptors still didn't respond out of ${acceptorsAddresses.size}")
        if (waitFor.size <= acceptorsAddresses.size / 2) {
          log.info(s"Decision is accepted by majority of acceptors")
          resender ! ("send", ballotProposal.slot, ballotProposal.command)
          context stop self
        }
      } else {
        leader ! ("preempted", ballot)
        context stop self
      }
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
