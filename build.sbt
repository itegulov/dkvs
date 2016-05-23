name := "dkvs"

version := "1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.6"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-actor" % "2.4.6",
  "com.typesafe.akka" %% "akka-stream" % "2.4.6",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.6",
  "com.typesafe.akka" %% "akka-remote" % "2.4.6",
  "com.github.kxbmap" %% "configs" % "0.4.2"
)
