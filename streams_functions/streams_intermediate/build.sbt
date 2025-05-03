name := "streams_intermediate"

import sbtassembly.MergeStrategy
import sbtassembly.PathList

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
  "org.slf4j"          %  "slf4j-simple"      % "1.7.36"
)
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion


assembly / assemblyMergeStrategy := {
  // concat all reference.conf and application.conf
  case PathList("reference.conf")   => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat

  // drop module-info.class
  case PathList("module-info.class") => MergeStrategy.discard

  // pick first of Netty’s versions file
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first

  // pick first of Akka version.conf (root-level)
  case PathList("version.conf")       => MergeStrategy.first

  // drop extra manifests
  case PathList("META-INF", xs @ _*) 
    if xs.lastOption.exists(_.equalsIgnoreCase("manifest.mf")) =>
      MergeStrategy.discard

  // everything else: default
  case other => (assembly / assemblyMergeStrategy).value(other)
}
