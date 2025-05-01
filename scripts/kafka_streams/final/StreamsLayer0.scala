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
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

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

  // Serdes
  implicit val byteArraySerde: Serdes.ByteArraySerde = new Serdes.ByteArraySerde
  implicit val collectorSerde: Serde[NeuronCollector] = Serdes.serdeFrom(
    new Serializer[NeuronCollector] {
      override def serialize(topic: String, data: NeuronCollector): Array[Byte] = {
        val buf = StreamsLayer0.threadBuffer.get()
        buf.clear()
        buf.putInt(data.count)
        data.values.foreach(buf.putDouble)
        val result = new Array[Byte](4 + data.values.length * 8)
        buf.rewind()
        buf.get(result)
        result
      }
    },
    new Deserializer[NeuronCollector] {
      override def deserialize(topic: String, bytes: Array[Byte]): NeuronCollector = {
        val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val count = buf.getInt()
        val values = Array.ofDim[Double](neuronCount)
        var i = 0; while (i < neuronCount) { values(i) = buf.getDouble(); i += 1 }
        NeuronCollector(values, count)
      }
    }
  )

  case class NeuronCollector(values: Array[Double], var count: Int)

  def serializePayload(arr: Array[Double]): Array[Byte] = {
    val buf = threadBuffer.get()
    buf.clear()
    arr.foreach(buf.putDouble)
    val result = new Array[Byte](arr.length * 8)
    buf.rewind()
    buf.get(result)
    result
  }

  val threadBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue(): ByteBuffer = ByteBuffer.allocateDirect(4096).order(ByteOrder.LITTLE_ENDIAN)
  }

  val builder = new StreamsBuilder()

  builder.addStateStore(
    Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(stateStoreName),
      stringSerde,
      collectorSerde
    )
  )

  builder
    .stream[String, String](inputTopic)
    .transformValues(() => new ValueTransformerWithKey[String, String, String] {
      private var store: KeyValueStore[String, NeuronCollector] = _
      private var redisAsync: RedisAsyncCommands[Array[Byte], Array[Byte]] = _

      override def init(context: org.apache.kafka.streams.processor.ProcessorContext): Unit = {
        store = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, NeuronCollector]]
        redisAsync = newAsyncRedis()
      }

      override def transform(id: String, v: String): String = {
        val sep = v.indexOf('|')
        if (sep < 0) return null
        val idx = v.substring(0, sep).toInt
        val dv  = v.substring(sep + 1).toDouble
        if (idx < 0 || idx >= neuronCount) return null

        val existing = store.get(id)
        if (existing != null && existing.count < 0) return null

        val collector = Option(existing).getOrElse(NeuronCollector(new Array[Double](neuronCount), 0))
        if (collector.values(idx) == 0.0) {
          collector.values(idx) = dv
          collector.count += 1
        }

        if (collector.count >= neuronCount) {
          val keyBytes = s"0_$id".getBytes(StandardCharsets.UTF_8)
          val payload  = serializePayload(collector.values)
          redisAsync.setex(keyBytes, 20, payload)
          store.delete(id)
          id
        } else {
          println(s"[STORE] Updating $id with count = ${collector.count}")
          store.put(id, collector)
          null
        }
      }

      override def close(): Unit = {
        if (redisAsync != null) redisAsync.getStatefulConnection.close()
      }
    }, stateStoreName)
    .filter((_, id) => id != null)
    .to(outputTopic)

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG,       "streams-layer0-app")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,    "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
  props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,   "10")
  props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, (64 * 1024 * 1024).toString)
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,    "5000")
  props.put("retention.bytes", "1073741824")
  props.put("segment.bytes", "134217728")

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

  cleanLocalState(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG))
  val streams = new KafkaStreams(builder.build(), props)
  streams.start()
  sys.addShutdownHook {
    streams.close()
    redisClient.shutdown()
  }
}
