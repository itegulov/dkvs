package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging}
import com.itegulov.dkvs.structure.{BallotNumber, BallotProposal, BallotProposals}
import com.itegulov.dkvs.structure.BallotNumber._

import scala.collection.mutable

/**
  * @author Daniyar Itegulov
  */
class Acceptor(val acceptorId: Int) extends Actor with ActorLogging {
  private var ballotNumber: BallotNumber = ⊥
  private val accepted = mutable.Set.empty[BallotProposal]

  override def receive: Receive = {
    case ("p1a", ballot: BallotNumber) =>
      log.info(s"New p1a request with ($ballot) arguments")
      if (ballot > ballotNumber) ballotNumber = ballot
      sender ! ("p1b", acceptorId, ballotNumber, BallotProposals(accepted.toSet))
    case ("p2a", ballotProposal: BallotProposal) =>
      log.info(s"New p2a request with ($ballotProposal) arguments")
      if (ballotProposal.ballot eq ballotNumber) accepted += ballotProposal
      sender ! ("p2b", acceptorId, ballotNumber)
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
