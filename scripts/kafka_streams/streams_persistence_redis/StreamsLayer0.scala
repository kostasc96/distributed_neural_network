package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.kstream.{ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import redis.clients.jedis.{Jedis, JedisPool}

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties

object StreamsLayer0 {

  val neuronCount: Int = 128
  val stateStoreName: String = "image-neuron-store"
  val redisPool = new JedisPool("localhost", 6379)

  case class NeuronCollector(values: Array[Double], var count: Int)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    val storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(stateStoreName),
      Serdes.String,
      Serdes.ByteArray
    )
    builder.addStateStore(storeBuilder)

    val sourceStream: KStream[String, String] = builder.stream[String, String]("layer-0-streams")

    val processedStream = sourceStream
      .transformValues(new ValueTransformerWithKeySupplier[String, String, String] {
        override def get(): ValueTransformerWithKey[String, String, String] =
          new ValueTransformerWithKey[String, String, String] {
            private var stateStore: KeyValueStore[String, Array[Byte]] = _
            private var jedis: Jedis = _  // ✅ Keep per-transformer Redis connection

            override def init(context: org.apache.kafka.streams.processor.ProcessorContext): Unit = {
              stateStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Array[Byte]]]
              jedis = redisPool.getResource
            }

            override def transform(imageId: String, value: String): String = {
              val sepIdx = value.indexOf('|')
              if (sepIdx == -1) return null

              val neuronId = value.substring(0, sepIdx).toInt
              val data = value.substring(sepIdx + 1).toDouble

              if (neuronId < 0 || neuronId >= neuronCount) return null

              val collector = Option(stateStore.get(imageId)) match {
                case Some(bytes) => deserializeCollector(bytes)
                case None        => NeuronCollector(new Array[Double](neuronCount), 0)
              }

              if (collector.values(neuronId) == 0.0d) {
                collector.values(neuronId) = data
                collector.count += 1
              }

              if (collector.count >= neuronCount) {
                val pipe = jedis.pipelined()
                pipe.set(s"0_$imageId".getBytes(), serializeDoubleArray(collector.values))
                pipe.expire(s"0_$imageId".getBytes(), 5)
                pipe.sync()

                stateStore.delete(imageId)
                imageId
              } else {
                stateStore.put(imageId, serializeCollector(collector))
                null
              }
            }

            override def close(): Unit = {
              if (jedis != null) {
                jedis.close()  // ✅ Return to pool on shutdown
              }
            }
          }
      }, stateStoreName)
      .filter((_, value) => value != null)

    processedStream.to("layer-1")

    val topology: Topology = builder.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-layer0-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "12")

    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, (64*1024*1024).toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.Serdes.StringSerde])

    cleanLocalState("streams-layer0-app")

    val streams = new KafkaStreams(topology, props)
    streams.start()

    sys.addShutdownHook {
      streams.close()
      redisPool.close()
    }
  }

  def serializeCollector(collector: NeuronCollector): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4 + collector.values.length * 8)
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN) // ✅ Match Redis + Python

    buffer.putInt(collector.count)
    collector.values.foreach(buffer.putDouble)
    buffer.array()
  }

  def deserializeCollector(bytes: Array[Byte]): NeuronCollector = {
    val buffer = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN)
    val count = buffer.getInt
    val values = new Array[Double](neuronCount)
    var i = 0
    while (i < neuronCount) {
      values(i) = buffer.getDouble
      i += 1
    }
    NeuronCollector(values, count)
  }

  def serializeDoubleArray(values: Array[Double]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.length * java.lang.Double.BYTES)
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    values.foreach(buffer.putDouble)
    buffer.array()
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