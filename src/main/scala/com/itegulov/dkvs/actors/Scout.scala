package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.itegulov.dkvs.structure.{Address, BallotNumber, BallotProposal, BallotProposals}

import scala.collection.mutable

/**
  * @author Daniyar Itegulov
  */
class Scout(ballotNumber: BallotNumber, acceptorsAddresses: Seq[Address], leader: ActorRef) extends Actor with ActorLogging {
  private val pvalues = mutable.Set.empty[BallotProposal]
  private val waitFor = mutable.Set(acceptorsAddresses.indices: _*)

  val acceptors = acceptorsAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Acceptors@${address.hostname}:${address.port}/user/Acceptor$i")
  }

  acceptors.foreach(acceptor => {
    acceptor ! ("p1a", ballotNumber)
  })

  override def receive: Receive = {
    case ("p1b", acceptorId: Int, ballot: BallotNumber, BallotProposals(accepted)) =>
      log.info(s"New p1b response with ($acceptorId, $ballot, $accepted) arguments")
      if (ballotNumber eq ballot) {
        pvalues ++= accepted
        waitFor -= acceptorId
        if (waitFor.size <= acceptorsAddresses.size / 2) {
          leader ! ("adopted", ballot, pvalues)
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
