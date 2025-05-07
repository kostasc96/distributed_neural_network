package app

import java.util.Properties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, Produced}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.clients.consumer.ConsumerConfig

object MirrorTopics extends App {

    val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", throw new IllegalArgumentException("KAFKA_BOOTSTRAP_SERVERS unset"))

    // Configure the Streams application
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "mirror-topics-app")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
      // Enable exactly-once processing if desired:
//       p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
      p
    }

    // Build the topology
    val builder = new StreamsBuilder()

    // Stream for layer-0 -> layer-0-mock
    val layer0Stream: KStream[String, String] = builder.stream[String, String]("layer-0")
    layer0Stream.to("layer-0-mock")(Produced.`with`(Serdes.String, Serdes.String))

    // Stream for layer-1 -> layer-1-mock
    val layer1Stream: KStream[String, String] = builder.stream[String, String]("layer-1")
    layer1Stream.to("layer-1-mock")(Produced.`with`(Serdes.String, Serdes.String))

    // Start the Streams application
    val topology = builder.build()
    val streams = new KafkaStreams(topology, props)

    // Graceful shutdown hook
    sys.ShutdownHookThread {
      streams.close()
    }

    streams.start()
    println("MirrorTopics started, forwarding layer-0 -> layer-0-mock and layer-1 -> layer-1-mock.")
}
