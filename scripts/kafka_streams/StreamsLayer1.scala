package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties

object StreamsLayer1 {

  val neuronCount: Int = 10
  val stateStoreName: String = "image-neuron-store"

  case class NeuronCollector(values: Array[Double], var count: Int)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // State store for partial accumulations
    val storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(stateStoreName),
      Serdes.String,
      Serdes.ByteArray
    )
    builder.addStateStore(storeBuilder)

    // Source stream
    val source: KStream[String, String] = builder.stream[String, String]("layer-1-streams")

    // Transformer: accumulate into NeuronCollector, emit on full batch
    val transformed = source
      .transformValues(new ValueTransformerWithKeySupplier[String, String, String] {
        override def get(): ValueTransformerWithKey[String, String, String] =
          new ValueTransformerWithKey[String, String, String] {
            private var stateStore: KeyValueStore[String, Array[Byte]] = _

            override def init(ctx: org.apache.kafka.streams.processor.ProcessorContext): Unit = {
              stateStore = ctx.getStateStore(stateStoreName)
                .asInstanceOf[KeyValueStore[String, Array[Byte]]]
            }

            override def transform(imageId: String, value: String): String = {
              val sepIdx = value.indexOf('|')
              if (sepIdx < 0) return null

              val neuronId = value.substring(0, sepIdx).toInt
              val data = value.substring(sepIdx + 1).toDouble

              if (neuronId < 0 || neuronId >= neuronCount) return null

              // Get or init collector
              val coll = Option(stateStore.get(imageId)) match {
                case Some(bytes) => deserializeCollector(bytes)
                case None        => NeuronCollector(new Array[Double](neuronCount), 0)
              }

              // Update
              if (coll.values(neuronId) == 0.0) {
                coll.values(neuronId) = data
                coll.count += 1
              }

              if (coll.count >= neuronCount) {
                // Choose highest-value neuron index
                val maxIdx = coll.values.zipWithIndex.maxBy(_._1)._2
                stateStore.delete(imageId)
                // Emit imageId|prediction
                s"$imageId|$maxIdx"
              } else {
                stateStore.put(imageId, serializeCollector(coll))
                null
              }
            }

            override def close(): Unit = {}
          }
      }, stateStoreName)
      .filter((_, v) => v != null)

    // Send to output topic
    transformed.to("layer-output")

    // Build and start
    val topology = builder.build()
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-layer1-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "10")
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, (64*1024*1024).toString)

    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "50")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")

    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
      classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])

    cleanLocalState("streams-layer1-app")

    val streams = new KafkaStreams(topology, props)
    streams.start()
    sys.addShutdownHook { streams.close() }
  }

  // Serialization helpers
  def serializeCollector(coll: NeuronCollector): Array[Byte] = {
    val buf = ByteBuffer.allocate(4 + coll.values.length * 8)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
    buf.putInt(coll.count)
    coll.values.foreach(buf.putDouble)
    buf.array()
  }

  def deserializeCollector(bytes: Array[Byte]): NeuronCollector = {
    val buf = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val cnt = buf.getInt
    val arr = new Array[Double](neuronCount)
    var i = 0
    while (i < neuronCount) {
      arr(i) = buf.getDouble
      i += 1
    }
    NeuronCollector(arr, cnt)
  }

  // Cleanup local RocksDB state
  def cleanLocalState(appId: String): Unit = {
    val dir = new File(s"/tmp/kafka-streams/$appId")
    if (dir.exists()) deleteRecursively(dir)
  }
  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }
}
