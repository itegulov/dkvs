package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.itegulov.dkvs.structure.{Address, BallotNumber}

import scala.collection.mutable

/**
  * @author Daniyar Itegulov
  */
class Leader(id: Int,
             acceptorsAddresses: Seq[Address],
             replicasAddresses: Seq[Address])(implicit system: ActorSystem) extends Actor with ActorLogging {
  var ballotNumber: BallotNumber = 0
  var active = false
  val proposals = mutable.Set.empty[(Int, Int)]

  system.actorOf(Props(new Scout(ballotNumber, acceptorsAddresses, self)))

  override def receive: Receive = {
    // TODO: implement
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
