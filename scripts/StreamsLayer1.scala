package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.util.Properties
import scala.collection.mutable

object StreamsLayer1 {

  // Number of occurrences required per image_id
  val neuronCount: Int = 10

  def main(args: Array[String]): Unit = {

    // Create the builder
    val builder = new StreamsBuilder()

    // In-memory map to count messages per image_id
    // (not fault-tolerant or distributed—just for demonstration)
    val counts = mutable.Map[String, Int]()

    // Create a stream from the "layer-0-streams" topic
    val sourceStream: KStream[String, String] = builder.stream[String, String]("layer-1-streams")

    // Use flatMap to optionally emit a record if the threshold is met.
    // For each incoming message, we:
    //   1) Increment the counter in `counts`.
    //   2) If the count >= neuronCount, remove from map and emit a record.
    //   3) Otherwise, emit nothing (Nil).
    val processedStream: KStream[String, String] = sourceStream.flatMap { (key, imageId) =>
      val newCount = counts.getOrElse(imageId, 0) + 1
      counts.update(imageId, newCount)
      if (newCount >= neuronCount) {
        counts.remove(imageId)  // This resets the count for the next batch.
        List((null, imageId))    // Emit a single record.
      } else {
        Nil
      }
    }

    // Output to "layer-1" topic
    processedStream.to("layer-output")

    // Build the topology
    val topology: Topology = builder.build()

    // Set up Streams configuration
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-layer1-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    // Add more configs (e.g., default key/value serdes) as needed

    // Start the Kafka Streams app
    val streams = new KafkaStreams(topology, props)
    streams.start()

    // Graceful shutdown
    sys.addShutdownHook {
      streams.close()
    }
  }
}