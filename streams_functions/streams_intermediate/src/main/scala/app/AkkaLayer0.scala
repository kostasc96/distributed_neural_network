package app

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.{ActorAttributes, Materializer, OverflowStrategy, SystemMaterializer}
import com.typesafe.config.ConfigFactory
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  // 1) Load all settings from src/main/resources/application.conf
  val config = ConfigFactory.load()

  implicit val system: ActorSystem  = ActorSystem("streams-intermediate", config)
  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.dispatcher

  private val redisDispatcher: MessageDispatcher =
    system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // 2) Read “flat” config values
  val neuronCount   = config.getInt("akka.stream.neuron-count")
  val parallelism   = config.getInt("akka.stream.parallelism")
  val bufferSize    = config.getInt("akka.stream.buffer-size")
  val batchSize     = config.getInt("akka.stream.batch-size")
  val batchDuration = config.getString("akka.stream.batch-duration") match {
    case s if s.endsWith("ms") => s.stripSuffix("ms").toInt.millis
    case s if s.endsWith("s")  => s.stripSuffix("s").toInt.seconds
    case _                      => throw new IllegalArgumentException("Invalid batch-duration")
  }
  val redisTtlSec   = config.getInt("akka.stream.redis-ttl-sec")

  val inputTopic   = config.getString("myapp.kafka.input-topic")
  val outputTopic  = config.getString("myapp.kafka.output-topic")
  val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", throw new IllegalArgumentException(s"Environment variable KAFKA_BOOTSTRAP_SERVERS not set"))
  val redisUri = sys.env.getOrElse("REDIS_URI", throw new IllegalArgumentException(s"Environment variable REDIS_URI not set"))

  // 3) Redis setup
  private val client: RedisClient                               = RedisClient.create(redisUri)
  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] = client.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]]     = asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)
  private val syncConn                                    = client.connect(ByteArrayCodec.INSTANCE)
  private val syncCmds                                    = syncConn.sync()

  private def doubleBytes(d: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(d).array()
  }

  // 4) Kafka settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaServers)
    .withGroupId("akka-streams0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,    bufferSize.toString)
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,  "100")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(kafkaServers)
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,           "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,         (2 * 1024 * 1024).toString)
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,   "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,               "1")

  // 5) Stream graph
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics(inputTopic))
    .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
    .map { msg =>
      val v   = msg.value()
      val sep = v.indexOf('|')
      (msg.key(), (v.substring(0, sep).toInt, v.substring(sep + 1).toDouble))
    }
    .buffer(bufferSize, OverflowStrategy.backpressure)
    .groupedWithin(batchSize, batchDuration)
    .mapAsyncUnordered(parallelism) { batch =>
      Future {
        val completed = ListBuffer.empty[String]
        val futures = batch
          .groupBy(_._1)
          .map { case (id, elems) =>
            val finalKey = s"0_$id".getBytes(StandardCharsets.UTF_8)
            val countKey = s"cnt:0:$id".getBytes(StandardCharsets.UTF_8)
            elems.foreach { case (_, (idx, dbl)) =>
              asyncCmds.setrange(finalKey, idx * 8L, doubleBytes(dbl))
            }
            val incrFut = asyncCmds.incrby(countKey, elems.size)
            (id, finalKey, countKey, incrFut)
          }
          .toList

        asyncCmds.flushCommands()
        futures.foreach { case (id, finalKey, countKey, incrFut) =>
          val cnt = incrFut.get()
          if (cnt >= neuronCount) {
            asyncCmds.expire(finalKey, redisTtlSec)
            asyncCmds.del(countKey)
            completed += id
          }
        }
        asyncCmds.flushCommands()
        completed.toList
      }(redisDispatcher)
    }
    .mapConcat(identity)
    .withAttributes(ActorAttributes.dispatcher("akka.actor.compute-dispatcher"))
    .map(id => new ProducerRecord[String, String](outputTopic, id, id))
    .runWith(Producer.plainSink(producerSettings.withDispatcher("akka.actor.compute-dispatcher")))
    .onComplete { _ =>
      asyncConn.close()
      syncConn.close()
      client.shutdown()
      system.terminate()
    }
}
