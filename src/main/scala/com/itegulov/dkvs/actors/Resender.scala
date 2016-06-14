package com.itegulov.dkvs.actors

import akka.actor.{Actor, ActorLogging, ActorSystem}
import com.itegulov.dkvs.structure.{Address, Command}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @author Daniyar Itegulov
  */
class Resender(replicasAddresses: Seq[Address], system: ActorSystem) extends Actor with ActorLogging {
  val replicas = replicasAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Replicas@${address.hostname}:${address.port}/user/Replica$i")
  }

  import context.dispatcher

  val queues = Array.tabulate[mutable.Queue[(Int, Command)]](replicasAddresses.size)(_ => mutable.Queue.empty)

  system.scheduler.schedule(0 seconds, 500 millis, self, "resend")

  override def receive: Receive = {
    case ("send", slot: Int, command: Command) =>
      queues.foreach(q => q += slot -> command)
      queues.zip(replicas).foreach { case (q, replica) =>
        if (q.size == 1) {
          val (slot, command) = q.front
          replica ! ("decision", slot, command)
        }
      }
    case ("acknowledge", slot: Int, id: Int) =>
      if (queues(id).front._1 == slot) {
        queues(id).dequeue()
      }
      if (queues(id).nonEmpty) {
        val (slot, command) = queues(id).front
        replicas(id) ! ("decision", slot, command)
      }
    case "resend" =>
      queues.zip(replicas).foreach { case (q, replica) =>
        if (q.nonEmpty) {
          val (slot, command) = q.front
          replica ! ("decision", slot, command)
        }
      }
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
