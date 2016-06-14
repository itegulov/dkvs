package com.itegulov.dkvs.actors

import java.io._
import java.util.{Scanner, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import com.itegulov.dkvs.structure.{Address, Command, DeleteCommand, SetCommand}
import resource._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.postfixOps

/**
  * @author Daniyar Itegulov
  */
class Replica(id: Int,
              system: ActorSystem,
              leadersAddresses: Seq[Address]) extends Actor with ActorLogging {
  val state = mutable.Map.empty[String, String]
  var slotIn = 1
  var slotOut = 1
  val requests = ArrayBuffer.empty[Command]
  val proposals = mutable.Map.empty[Int, Command]
  val decisions = mutable.Map.empty[Int, Command]
  val senders = mutable.Map.empty[Int, ActorRef]
  // Helper state
  val clientIdToActor = mutable.WeakHashMap.empty[UUID, ActorRef]
  val logOut = new PrintWriter(new FileOutputStream(s"log/dkvs_replica_$id.log", true), true)

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
        log.info(s"Proposing to leaders $slotIn -> $command")
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
            logOut.println(s"set $key $value")
          case DeleteCommand(key, clientId) =>
            val client = clientIdToActor.get(clientId)
            if (state.contains(key)) {
              state -= key
              client.foreach(_ ! ("deleteAnswer", "deleted"))
            } else {
              client.foreach(_ ! ("deleteAnswer", "notFound"))
            }
            logOut.println(s"delete $key")
        }
        log.info(s"Performed $command")
        slotOut += 1
    }
  }

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }


  @throws(classOf[Exception])
  override def preStart(): Unit = {
    val lines = Source.fromFile(s"log/dkvs_replica_$id.log").getLines()
    var read = 0
    for (line <- lines) {
      line match {
        case r"^set (.*)$key (.*)$value" => {
          state += key -> value
          read += 1
        }
        case r"^delete (.*)$key" => {
          state -= key
          read += 1
        }
        case _ => throw new IllegalStateException("Log is malformed")
      }
    }
    slotIn = read + 1
    slotOut = read + 1
    log.info(s"Restored from log $read lines")
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
      if (slot < slotOut) {
        sender ! ("acknowledge", slot, id)
      } else {
        decisions += slot -> command
        senders += slot -> sender
        while (decisions.contains(slotOut)) {
          proposals.get(slotOut) match {
            case Some(oldCommand) =>
              proposals -= slotOut
              if (oldCommand != command) requests += oldCommand
            case None =>
          }
          val current = slotOut
          perform(command)
          senders(current) ! ("acknowledge", slot, id)
        }
      }
    case "propose" =>
      propose()
    case other =>
      log.warning(s"Got something strange: $other")
  }
}
