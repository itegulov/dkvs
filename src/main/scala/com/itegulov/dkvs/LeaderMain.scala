package com.itegulov.dkvs

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.itegulov.dkvs.actors.{Leader, Resender, Scout}
import com.itegulov.dkvs.structure.Address
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import configs.Configs
import configs.Result.{Failure, Success}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

/**
  * @author Daniyar Itegulov
  */
object LeaderMain extends App {
  if (args.length != 1) {
    println("Usage: LeaderMain <number>")
    sys.exit(1)
  }
  val nodeNumber = Try(args(0).toInt).getOrElse({
    println(s"${args(0)} is not a valid number")
    sys.exit(1)
  })
  System.setProperty("node", s"leader_$nodeNumber")
  val dkvsConfig = ConfigFactory.load("dkvs")
  val t = Configs[Int].get(dkvsConfig, "dkvs.timeout") match {
    case Success(time) => time
    case _ =>
      println("Timeout wasn't specified")
      sys.exit(1)
  }
  implicit val timeout = Timeout(t millis)
  Configs[Address].get(dkvsConfig, s"dkvs.leaders.$nodeNumber") match {
    case Success(address) =>

      val acceptorsAddresses = Configs[Seq[Address]].get(dkvsConfig, "dkvs.acceptors").valueOrElse({
        println(s"Couldn't find configuration of acceptors")
        sys.exit(1)
      })

      val replicasAddresses = Configs[Seq[Address]].get(dkvsConfig, "dkvs.replicas").valueOrElse({
        println(s"Couldn't find configuration of replicas")
        sys.exit(1)
      })

      val leaderConfig = ConfigFactory.load("common")
        .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(address.port))
        .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(address.hostname))

      implicit val system = ActorSystem("Leaders", leaderConfig)
      val resenderActor = system.actorOf(Props(new Resender(replicasAddresses, system)))
      val leaderActor = system.actorOf(Props(new Leader(nodeNumber, acceptorsAddresses, resenderActor)), name = s"Leader$nodeNumber")

      Await.ready(system.whenTerminated, Duration.Inf)
    case Failure(error) =>
      println(s"Couldn't find configuration for leader $nodeNumber:")
      error.messages.foreach(println)
      sys.exit(1)
  }
}
