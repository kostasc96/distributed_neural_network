package app

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

/**
 * Optimized Kafka Streams application mirroring Akka Streams logic,
 * with idempotent emission only upon first complete vector per image ID.
 */
object StreamsLayer0 extends App {
  val neuronCount      = 128
  val inputTopic       = "layer-0-streams"
  val outputTopic      = "layer-1"
  val stateStoreName   = "image-neuron-store"

  // Redis setup
  private val redisClient = RedisClient.create(
    RedisURI.Builder.redis("localhost", 6379)
      .withTimeout(Duration.ofSeconds(5)).build()
  )
  def newAsyncRedis(): RedisAsyncCommands[Array[Byte], Array[Byte]] =
    redisClient.connect(ByteArrayCodec.INSTANCE).async()

  // State serialization helpers
  case class NeuronCollector(values: Array[Double], var count: Int)

  private def serializeCollector(c: NeuronCollector): Array[Byte] = {
    val buf = ByteBuffer.allocate(4 + c.values.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(c.count)
    c.values.foreach(buf.putDouble)
    buf.array()
  }

  private def serializePayload(arr: Array[Double]): Array[Byte] = {
    val buf = ByteBuffer.allocate(arr.length * java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    arr.foreach(buf.putDouble)
    buf.array()
  }

  // Build topology
  val builder = new StreamsBuilder()

  // In-memory state store (no changelog)
  builder.addStateStore(
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore(stateStoreName),
      stringSerde,
      byteArraySerde
    ).withLoggingDisabled()
  )

  builder
    .stream[String, String](inputTopic)
    .transformValues(() => new ValueTransformerWithKey[String, String, String] {
      private var store: KeyValueStore[String, Array[Byte]] = _
      private var redisAsync: RedisAsyncCommands[Array[Byte], Array[Byte]] = _

      override def init(context: org.apache.kafka.streams.processor.ProcessorContext): Unit = {
        store = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, Array[Byte]]]
        redisAsync = newAsyncRedis()
      }

      override def transform(id: String, v: String): String = {
        // idempotency: if marked processed, skip
        Option(store.get(id)).foreach { raw =>
          val cnt = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).getInt()
          if (cnt < 0) return null
        }
        // parse "idx|value"
        val sep = v.indexOf('|')
        if (sep < 0) return null
        val idx = v.substring(0, sep).toInt
        val dv  = v.substring(sep + 1).toDouble
        if (idx < 0 || idx >= neuronCount) return null

        // load or init collector
        val (collector, count) = Option(store.get(id)).fold {
          (NeuronCollector(new Array[Double](neuronCount), 0), 0)
        } { raw =>
          val buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
          val c   = buf.getInt()
          val arr = Array.ofDim[Double](neuronCount)
          var i = 0; while (i < neuronCount) { arr(i) = buf.getDouble(); i += 1 }
          (NeuronCollector(arr, c), c)
        }

        if (collector.values(idx) == 0.0) {
          collector.values(idx) = dv
          collector.count += 1
        }

        if (collector.count >= neuronCount) {
          // write to Redis
          val keyBytes = s"0_$id".getBytes(StandardCharsets.UTF_8)
          val payload  = serializePayload(collector.values)
          redisAsync.setex(keyBytes, 20, payload)
          // mark processed (count = -1)
          store.delete(id)
          id
        } else {
          // save partial collector
          store.put(id, serializeCollector(collector))
          null
        }
      }

      override def close(): Unit = {
        if (redisAsync != null) redisAsync.getStatefulConnection.close()
      }
    }, stateStoreName)
    .filter((_, id) => id != null)
    .to(outputTopic)

  // Streams configuration
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,       "streams-layer0-app")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,    "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
  props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,   "10")
  props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, (64 * 1024 * 1024).toString)
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,    "5000")

  props.put(ProducerConfig.BATCH_SIZE_CONFIG,           "131072")
  props.put(ProducerConfig.LINGER_MS_CONFIG,            "10")
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,     "lz4")

  props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,      "1048576")
  props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,    "100")

  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   classOf[Serdes.StringSerde])
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[Serdes.StringSerde])

  def cleanLocalState(appId: String): Unit = {
    val dir = new File(s"/tmp/kafka-streams/$appId")
    if (dir.exists()) deleteRecursively(dir)
  }
  def deleteRecursively(f: File): Unit = if (f.isDirectory) f.listFiles().foreach(deleteRecursively) else f.delete()

  // start
  cleanLocalState(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG))
  val streams = new KafkaStreams(builder.build(), props)
  streams.start()
  sys.addShutdownHook {
    streams.close()
    redisClient.shutdown()
  }
}
