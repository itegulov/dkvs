package com.itegulov.dkvs

import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Flow, Framing, Source, Tcp}
import akka.util.{ByteString, Timeout}
import com.itegulov.dkvs.actors.Replica
import com.itegulov.dkvs.structure.Address
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import configs.Configs
import configs.Result.{Failure, Success}

import scala.concurrent.duration._
import scala.concurrent._
import scala.language.postfixOps
import scala.util.Try

/**
  * @author Daniyar Itegulov
  */
object ReplicaMain extends App {
  if (args.length != 1) {
    println("Usage: ReplicaMain <number>")
    sys.exit(1)
  }
  val nodeNumber = Try(args(0).toInt).getOrElse({
    println(s"${args(0)} is not a valid number")
    sys.exit(1)
  })
  System.setProperty("node", s"replica_$nodeNumber")
  val dkvsConfig = ConfigFactory.load("dkvs")
  val t = Configs[Int].get(dkvsConfig, "dkvs.timeout") match {
    case Success(time) => time
    case _ =>
      println("Timeout wasn't specified")
      sys.exit(1)
  }
  implicit val timeout = Timeout(t millis)
  Configs[Address].get(dkvsConfig, s"dkvs.replicas.$nodeNumber") match {
    case Success(address) =>

      val leadersAddresses = Configs[Seq[Address]].get(dkvsConfig, "dkvs.leaders").valueOrElse({
        println(s"Couldn't find configuration of leaders")
        sys.exit(1)
      })

      val replicaConfig = ConfigFactory.load("common")
        .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(address.port))
        .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(address.hostname))

      implicit val system = ActorSystem("Replicas", replicaConfig)
      implicit val materializer = ActorMaterializer()
      val replicaActor = system.actorOf(Props(new Replica(nodeNumber, system, leadersAddresses)), name = s"Replica$nodeNumber")

      val connections: Source[IncomingConnection, Future[ServerBinding]] = Tcp().bind(address.hostname, 13000 + nodeNumber)
      connections runForeach { connection =>
        val echo = Flow[ByteString]
          .via(Framing.delimiter(
            ByteString("\n"),
            maximumFrameLength = 256,
            allowTruncation = true))
          .map(_.utf8String)
          .map {
            case GetRequest(key) =>
              val future = replicaActor ? ("get", key)
              val result = Await.result(future, Duration.Inf).asInstanceOf[(String, Any)]
              result match {
                case ("getAnswer", answer: String) =>
                  answer
                case ("getAnswer", None) =>
                  "NOT_FOUND"
                case _ =>
                  "INVALID_STATE"
              }
            case SetRequest(key, value) =>
              val future = replicaActor ? ("set", key, value)
              val result = Await.result(future, Duration.Inf).asInstanceOf[(String, String)]
              result match {
                case ("setAnswer", "stored") =>
                  "STORED"
                case _ =>
                  "INVALID_STATE"
              }
            case DeleteRequest(key) =>
              val future = replicaActor ? ("delete", key)
              val result = Await.result(future, Duration.Inf).asInstanceOf[(String, String)]
              result match {
                case ("deleteAnswer", "deleted") =>
                  "DELETED"
                case ("deleteAnswer", "notFound") =>
                  "NOT_FOUND"
                case _ =>
                  "INVALID_STATE"
              }
            case PingRequest() =>
              "PONG"
            case _ =>
              "INVALID_REQUEST"
          }
          .map(_ + "\n")
          .map(ByteString(_))

        connection.handleWith(echo)
      }

      Await.ready(system.whenTerminated, Duration.Inf)
    case Failure(error) =>
      println(s"Couldn't find configuration for replica $nodeNumber:")
      error.messages.foreach(println)
      sys.exit(1)
  }

  object GetRequest {
    def unapply(s: String): Option[String] = {
      if (s.startsWith("get ")) Some(s.stripPrefix("get "))
      else None
    }
  }

  object SetRequest {
    def unapply(s: String): Option[(String, String)] = {
      if (s.startsWith("set ")) {
        val args = s.stripPrefix("set ").split(" ")
        if (args.length != 2) {
          None
        } else {
          Some(args(0), args(1))
        }
      } else {
        None
      }
    }
  }

  object DeleteRequest {
    def unapply(s: String): Option[String] = {
      if (s.startsWith("delete ")) Some(s.stripPrefix("delete "))
      else None
    }
  }

  object PingRequest {
    def unapply(s: String): Boolean = s == "ping"
  }
}
