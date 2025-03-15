name := "ScalaJupyterAkka"

version := "0.1"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
  "com.typesafe.akka" %% "akka-stream" % "2.6.21",
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.21",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.2"


resolvers += "Akka Repository" at "https://repo.akka.io/releases/"
