package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.itegulov.dkvs.structure._

import scala.collection.mutable

/**
  * @author Daniyar Itegulov
  */
class Leader(id: Int,
             acceptorsAddresses: Seq[Address],
             resender: ActorRef)
            (implicit val system: ActorSystem,
             implicit val timeout: Timeout) extends Actor with ActorLogging {
  var ballotNumber: BallotNumber = 0
  var active = false
  var proposals = Map.empty[Int, Command]

  system.actorOf(Props(new Scout(ballotNumber, acceptorsAddresses, self)))

  private def pmax(pvalues: Set[BallotProposal]): Map[Int, Command] = {
    val map = mutable.Map.empty[Int, mutable.Set[(BallotNumber, Command)]]
    for (BallotProposal(ballot, slot, command) <- pvalues) {
      if (!map.contains(slot)) map += slot -> mutable.Set.empty
      map(slot) += ballot -> command
    }
    map.mapValues(set => set.maxBy {
      case (ballot, _) => ballot
    } match {
      case (_, command) => command
    }).toMap
  }

  private implicit class AdditionalMapOps[K, V](lhs: Map[K, V]) {
    def ◁(rhs: Map[K, V]): Map[K, V] = {
      val diff = lhs.flatMap { case (key, value) => if (rhs contains key) None else Some(key -> value) }
      (diff.toSeq ++ rhs.toSeq).toMap
    }
  }

  override def receive: Receive = {
    case ("propose", slot: Int, command: Command) =>
      log.info(s"New propose request with ($slot, $command) arguments")
      if (!proposals.contains(slot)) {
        proposals += slot -> command
        log.info(s"Proposal $slot -> $command was added to proposals queue")
        if (active) {
          log.info(s"Creating commander for proposal $slot -> $command")
          system.actorOf(Props(new Commander(BallotProposal(ballotNumber, slot, command), acceptorsAddresses, self, resender)))
        }
      } else {
        log.info(s"Ignoring proposal $slot -> $command because already have command for this slot")
      }
    case ("adopted", ballot: BallotNumber, BallotProposals(pvalues)) =>
      log.info(s"New adopted response with ($ballot, $pvalues) arguments")
      proposals = proposals ◁ pmax(pvalues)
      proposals.foreach {
        case (slot, command) =>
          system.actorOf(Props(new Commander(BallotProposal(ballotNumber, slot, command), acceptorsAddresses, self, resender)))
      }
      """""".r.findAllIn("").map(s => (1, 2)).toSeq
      active = true
    case ("preempted", ballot: BallotNumber) =>
      log.info(s"New preempted response with ($ballot) arguments")
      if (ballot > ballotNumber) {
        active = false
        ballotNumber = ballot + 1
        system.actorOf(Props(new Scout(ballotNumber, acceptorsAddresses, self)))
      }
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
