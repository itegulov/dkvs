name := "dkvs"

version := "1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.4.6"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.github.kxbmap" %% "configs" % "0.4.2",
  "com.jsuereth" %% "scala-arm" % "1.4"
)
