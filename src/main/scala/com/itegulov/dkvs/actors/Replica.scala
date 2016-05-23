package com.itegulov.dkvs.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import com.itegulov.dkvs.structure.{Address, Command, DeleteCommand, SetCommand}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

/**
  * @author Daniyar Itegulov
  */
class Replica(system: ActorSystem,
              leadersAddresses: Seq[Address]) extends Actor with ActorLogging {
  val state = mutable.Map.empty[String, String]
  var slotIn = 1
  var slotOut = 1
  val requests = ArrayBuffer.empty[Command]
  val proposals = mutable.Map.empty[Int, Command]
  val decisions = mutable.Map.empty[Int, Command]
  // Helper state
  val clientIdToActor = mutable.WeakHashMap.empty[UUID, ActorRef]

  val leaders = leadersAddresses.zipWithIndex.map {
    case (address, i) =>
      context.actorSelection(s"akka.tcp://Leaders@${address.hostname}:${address.port}/user/Leader$i")
  }

  system.scheduler.schedule(0 seconds, 30 millis, self, "propose") // FIXME: dirty hack to do some background work

  private def propose(): Unit = {
    while (slotIn < slotOut + 10 && requests.nonEmpty) {
      val command = requests(0)
      if (!decisions.contains(slotIn)) {
        requests.remove(0)
        proposals += slotIn -> command
        leaders.foreach(leader => leader ! ("propose", slotIn, command))
      }
      slotIn += 1
    }
  }

  private def perform(command: Command): Unit = {
    log.info(s"Trying to perform $command")
    decisions.find { case (s, c) => s < slotOut && c == command } match {
      case Some(_) =>
        log.info(s"$command was already performed")
        slotOut += 1
      case None =>
        command match {
          case SetCommand(key, value, clientId) =>
            state += key -> value
            val client = clientIdToActor.get(clientId)
            client.foreach(_ ! ("setAnswer", "stored"))
          case DeleteCommand(key, clientId) =>
            val client = clientIdToActor.get(clientId)
            if (state.contains(key)) {
              state -= key
              client.foreach(_ ! ("deleteAnswer", "deleted"))
            } else {
              client.foreach(_ ! ("deleteAnswer", "notFound"))
            }
        }
        log.info(s"Performed $command")
        slotOut += 1
    }
  }

  override def receive: Receive = {
    case ("get", key: String) =>
      log.info(s"New get request with ($key) arguments")
      state.get(key) match {
        case Some(answer) => sender ! ("getAnswer", answer)
        case None => sender ! ("getAnswer", None)
      }
    case ("set", key: String, value: String) =>
      log.info(s"New set request with ($key, $value) arguments")
      val clientId = UUID.randomUUID()
      requests += SetCommand(key, value, clientId)
      clientIdToActor += clientId -> sender
    case ("delete", key: String) =>
      log.info(s"New delete request with ($key) arguments")
      val clientId = UUID.randomUUID()
      requests += DeleteCommand(key, clientId)
      clientIdToActor += clientId -> sender
    case ("decision", slot: Int, command: Command) =>
      log.info(s"New decision request with ($slot, $command) arguments")
      decisions += slot -> command
      while (decisions.contains(slotOut)) {
        proposals.get(slotOut) match {
          case Some(oldCommand) =>
            proposals -= slotOut
            if (oldCommand != command) requests += oldCommand
          case None =>
        }
        perform(command)
      }
    case "propose" =>
      propose()
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
