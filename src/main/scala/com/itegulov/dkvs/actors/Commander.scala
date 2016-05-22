package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.itegulov.dkvs.structure.{Address, BallotNumber, BallotProposal}

import scala.collection.mutable

/**
  * @author Daniyar Itegulov
  */
class Commander(ballotProposal: BallotProposal,
                acceptorsAddresses: Seq[Address],
                replicasAddresses: Seq[Address],
                leader: ActorRef) extends Actor with ActorLogging {
  private val waitFor = mutable.Set(acceptorsAddresses.indices: _*)

  log.info(s"New commander has been created with " +
    s"($ballotProposal, $acceptorsAddresses, $replicasAddresses, $leader) arguments")

  val acceptors = acceptorsAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Acceptors@${address.hostname}:${address.port}/user/Acceptor$i")
  }

  val replicas = replicasAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Replicas@${address.hostname}:${address.port}/user/Replica$i")
  }

  acceptors.foreach(acceptor => {
    acceptor ! ("p2a", ballotProposal)
  })

  override def receive: Receive = {
    case ("p2b", acceptorId: Int, ballot: BallotNumber) =>
      log.info(s"New p2b response with ($acceptorId, $ballot) arguments")
      if (ballotProposal.ballot == ballot) {
        waitFor -= acceptorId
        log.info(s"${waitFor.size} acceptors still didn't respond out of ${acceptorsAddresses.size}")
        if (waitFor.size <= acceptorsAddresses.size / 2) {
          log.info(s"Decision is accepted by majority of acceptors")
          replicas.foreach(replica => replica ! ("decision", ballotProposal.slot, ballotProposal.command))
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
