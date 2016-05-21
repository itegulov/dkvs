package com.itegulov.dkvs

import akka.actor.{ActorSystem, Props}
import com.itegulov.dkvs.actors.{Leader, Scout}
import com.itegulov.dkvs.structure.Address
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import configs.Configs
import configs.Result.{Failure, Success}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
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
  val dkvsConfig = ConfigFactory.load("dkvs")
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
      val leaderActor = system.actorOf(Props(new Leader(nodeNumber, acceptorsAddresses, replicasAddresses)), name = s"Leader$nodeNumber")

      Await.ready(system.whenTerminated, Duration.Inf)
    case Failure(error) =>
      println(s"Couldn't find configuration for leader $nodeNumber:")
      error.messages.foreach(println)
      sys.exit(1)
  }
}
