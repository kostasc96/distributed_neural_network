name := "akka-essentials"

version := "0.1"

scalaVersion := "2.12.7"

val akkaVersion = "2.6.20"
val kafkaVersion = "3.6.0"
val scalaKafkaStreamsVersion = "3.6.0"

libraryDependencies ++= Seq(
  // Akka Core and TestKit
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,

  // Akka Streams
//  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http"    % "10.0.11",

  // ScalaTest for Testing
  "org.scalatest" %% "scalatest" % "3.0.5",

  // Redis (Jedis)
  "redis.clients" % "jedis" % "4.4.3",

  // Kafka Client
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,

  "org.apache.kafka" %% "kafka-streams-scala" % scalaKafkaStreamsVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  // JSON (Optional for better JSON handling)
  "com.typesafe.play" %% "play-json" % "2.6.10",

  "org.apache.avro"   %  "avro"          % "1.10.2"

)

// SLF4J Simple Logger (for simple console output)
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.4"
libraryDependencies += "com.google.flatbuffers" % "flatbuffers-java" % "2.0.0"
libraryDependencies += "software.amazon.awssdk" % "s3" % "2.25.0"
//libraryDependencies +=  "com.typesafe.akka" %% "akka-stream-kafka" % "1.1.0"
libraryDependencies += "io.lettuce" % "lettuce-core" % "6.2.0.RELEASE"
libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
libraryDependencies += "com.github.ben-manes.caffeine" % "caffeine" % "3.1.6"

libraryDependencies ++= Seq(
  // Akka Streams & Kafka
  "com.typesafe.akka" %% "akka-stream"           % "2.6.20",
  "com.typesafe.akka" %% "akka-stream-kafka"    % "2.1.1",
  // Alpakka S3 + AWS SDK v2
  "com.lightbend.akka" %% "akka-stream-alpakka-s3" % "3.0.4",
  "software.amazon.awssdk" % "netty-nio-client" % "2.17.116",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.4"
)
