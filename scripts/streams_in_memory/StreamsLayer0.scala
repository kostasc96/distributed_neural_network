package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import java.util.Properties

object StreamsLayer0 {

  // Number of occurrences required per image_id
  val neuronCount: Int = 128
  val stateStoreName: String = "counts-0"

  def main(args: Array[String]): Unit = {

    // Create the builder
    val builder = new StreamsBuilder()

    val storeBuilder = Stores.keyValueStoreBuilder(
      // This creates an in-memory key-value store
      Stores.inMemoryKeyValueStore(stateStoreName),
      org.apache.kafka.common.serialization.Serdes.String,
      org.apache.kafka.common.serialization.Serdes.Integer
    )
    builder.addStateStore(storeBuilder)


    // Create a stream from the "layer-0-streams" topic
    val sourceStream: KStream[String, String] = builder.stream[String, String]("layer-0-streams")

    val processedStream: KStream[String, String] = sourceStream.transform(
      new TransformerSupplier[String, String, KeyValue[String, String]] {
        override def get(): Transformer[String, String, KeyValue[String, String]] =
          new Transformer[String, String, KeyValue[String, String]] {
            private var stateStore: KeyValueStore[String, Int] = _
            private var context: ProcessorContext = _

            override def init(ctx: ProcessorContext): Unit = {
              context = ctx
              // Retrieve the in-memory state store
              stateStore = ctx.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Int]]
            }

            override def transform(key: String, imageId: String): KeyValue[String, String] = {
              // Increment the counter for this imageId
              val currentCount = Option(stateStore.get(imageId)).getOrElse(0)
              val newCount = currentCount + 1
              stateStore.put(imageId, newCount)

              // If the count reaches the desired threshold, emit the record and delete the count
              if (newCount >= neuronCount) {
                stateStore.delete(imageId)
                new KeyValue(null, imageId) // Emitting with a null key
              } else {
                null  // Return null to indicate that nothing is emitted
              }
            }

            override def close(): Unit = {}
          }
      },
      stateStoreName  // Register the state store with the transformer
    ).filter((_, value) => value != null)

    // Output to "layer-1" topic
    processedStream.to("layer-1")

    // Build the topology
    val topology: Topology = builder.build()

    // Set up Streams configuration
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-layer0-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "8")
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