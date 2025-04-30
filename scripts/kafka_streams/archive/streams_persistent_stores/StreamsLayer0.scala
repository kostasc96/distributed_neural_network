package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._

import java.io.File
import java.util.Properties

object StreamsLayer0 {

  val neuronCount: Int = 128
  val stateStoreName: String = "counts-0"

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // In-memory state store for imageId -> count
    val storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(stateStoreName),
      Serdes.String,
      Serdes.Integer
    )
    builder.addStateStore(storeBuilder)

    val sourceStream: KStream[String, String] = builder.stream[String, String]("layer-0-streams")

    val processedStream = sourceStream
      .transformValues(new ValueTransformerWithKeySupplier[String, String, String] {
        override def get(): ValueTransformerWithKey[String, String, String] =
          new ValueTransformerWithKey[String, String, String] {
            private var stateStore: KeyValueStore[String, Integer] = _

            override def init(context: org.apache.kafka.streams.processor.ProcessorContext): Unit = {
              stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Integer]]
            }

            override def transform(imageId: String, value: String): String = {
              val current: Int = Option(stateStore.get(imageId)).map(_.toInt).getOrElse(0)
              val updated: Int = current + 1

              if (updated >= neuronCount) {
                stateStore.delete(imageId)
                imageId
              } else {
                stateStore.put(imageId, updated)
                null
              }
            }

            override def close(): Unit = {}
          }
      }, stateStoreName)
      .filter((_, value) => value != null)

    // Forward to layer-1
    processedStream.to("layer-1")

    // Build topology
    val topology: Topology = builder.build()

    // Stream config
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-layer0-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "8")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])

    cleanLocalState("streams-layer0-app")
    // Start the stream
    val streams = new KafkaStreams(topology, props)
    streams.start()

    sys.addShutdownHook {
      streams.close()
    }
  }

  def cleanLocalState(appId: String): Unit = {
    val localStateDir = new File(s"/tmp/kafka-streams/$appId")
    if (localStateDir.exists()) {
      println(s"[INFO] Cleaning up local RocksDB state at ${localStateDir.getAbsolutePath}")
      deleteRecursively(localStateDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
