name := "mirror_topics"

import sbtassembly.MergeStrategy
import sbtassembly.PathList

version := "0.1"

scalaVersion := "2.12.7"

val kafkaVersion = "3.6.0"

libraryDependencies ++= Seq(
  "org.apache.kafka"   %  "kafka-clients"        % kafkaVersion,
  "org.slf4j"          %  "slf4j-simple"         % "1.7.36",
  "org.apache.kafka" %% "kafka-streams-scala"   % kafkaVersion,
  // … your other deps …
)

Compile / mainClass := Some("app.MirrorTopics")
assembly / mainClass := Some("app.MirrorTopics")

assembly / assemblyMergeStrategy := {
  // concatenate all reference.conf and application.conf
  case PathList("reference.conf")    => MergeStrategy.concat
  case PathList("application.conf")  => MergeStrategy.concat

  // discard the JAR‐module metadata under Java 9 multirelease
  case PathList("META-INF", "versions", "9", "module-info.class") =>
    MergeStrategy.discard

  // also drop any top‐level module-info.class (just in case)
  case PathList("module-info.class") =>
    MergeStrategy.discard

  // pick first of Netty’s versions file
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.first

  // pick first of Akka version.conf (root‐level)
  case PathList("version.conf") =>
    MergeStrategy.first

  // drop extra manifests
  case PathList("META-INF", xs @ _*)
    if xs.lastOption.exists(_.equalsIgnoreCase("manifest.mf")) =>
      MergeStrategy.discard

  // everything else: use default merge strategy
  case other =>
    (assembly / assemblyMergeStrategy).value(other)
}
