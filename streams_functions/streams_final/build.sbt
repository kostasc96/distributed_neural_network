name := "streams_final"

version := "0.1"

scalaVersion := "2.12.7"

val akkaVersion           = "2.6.20"
val alpakkaKafkaVersion   = "2.1.1"
val kafkaClientsVersion   = "3.6.0"
val lettuceVersion        = "6.2.0.RELEASE"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"        % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"       % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaVersion,
  "org.apache.kafka"   %  "kafka-clients"     % kafkaClientsVersion,
  "io.lettuce"         %  "lettuce-core"      % lettuceVersion,
  "org.slf4j"          %  "slf4j-simple"      % "1.7.36",
  "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "3.0.4",
  "software.amazon.awssdk" % "netty-nio-client" % "2.17.116"
)
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
libraryDependencies += "software.amazon.awssdk" % "s3" % "2.25.0"


Compile / mainClass := Some("app.AkkaLayer1")
assembly  / mainClass := Some("app.AkkaLayer1")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
