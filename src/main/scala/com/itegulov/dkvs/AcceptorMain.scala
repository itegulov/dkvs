package com.itegulov.dkvs

import akka.actor.{ActorSystem, Props}
import com.itegulov.dkvs.actors.Acceptor
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
object AcceptorMain extends App {
  if (args.length != 1) {
    println("Usage: AcceptorMain <number>")
    sys.exit(1)
  }
  val nodeNumber = Try(args(0).toInt).getOrElse({
    println(s"${args(0)} is not a valid number")
    sys.exit(1)
  })
  System.setProperty("node", s"acceptor_$nodeNumber")
  val dkvsConfig = ConfigFactory.load("dkvs")
  Configs[Address].get(dkvsConfig, s"dkvs.acceptors.$nodeNumber") match {
    case Success(address) =>

      val acceptorConfig = ConfigFactory.load("common")
        .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(address.port))
        .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(address.hostname))

      implicit val system = ActorSystem("Acceptors", acceptorConfig)
      val acceptorActor = system.actorOf(Props(new Acceptor(0)), name = s"Acceptor$nodeNumber")

      Await.ready(system.whenTerminated, Duration.Inf)
    case Failure(error) =>
      println(s"Couldn't find configuration for acceptor $nodeNumber:")
      error.messages.foreach(println)
      sys.exit(1)
  }
}
