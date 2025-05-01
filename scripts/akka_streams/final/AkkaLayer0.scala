package app

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorAttributes, Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  // 1) STREAM + DISPATCHER SETUP
  val conf = ConfigFactory.parseString("""
    akka.stream.materializer.initial-input-buffer-size = 64
    akka.stream.materializer.max-input-buffer-size     = 256

    akka.actor.redis-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 8 }
      throughput = 1
    }
    akka.actor.kafka-consumer-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 16 }
      throughput = 1
    }
  """).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem  = ActorSystem("single-key-redis", conf)
  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.dispatcher

  private val redisDispatcher: MessageDispatcher =
    system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // 2) CONSTANTS
  val neuronCount = 128           // fixed
  val parallelism = 16
  val inputTopic  = "layer-0-streams"
  val outputTopic = "layer-1"

  // 3) LETTUCE: ONE PIPELINED ASYNC & ONE SYNC CONNECTION
  private val client = RedisClient.create("redis://localhost:6379")

  // async (pipelined) connection
  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    client.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] =
    asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)

  // sync for incrby futures only (no GET)
  private val syncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    client.connect(ByteArrayCodec.INSTANCE)
  private val syncCmds = syncConn.sync()

  // helper to turn a Double into 8 little-endian bytes
  private def doubleBytes(d: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(d).array()
  }

  // 4) KAFKA CONSUMER & PRODUCER SETTINGS
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("akka-streams0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,     "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,   "100")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,           "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,          "2097152")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                "1")

  // 5) STREAM GRAPH
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics(inputTopic))
    .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
    .map { msg =>
      val v   = msg.value()
      val sep = v.indexOf('|')
      (msg.key(), (v.substring(0, sep).toInt, v.substring(sep + 1).toDouble))
    }
    .buffer(1024, OverflowStrategy.backpressure)
    .groupedWithin(500, 200.millis)
    .mapAsyncUnordered(parallelism) { batch =>
      Future {
        val completed = ListBuffer.empty[String]

        // 5a) Pipeline all SETRANGE + INCRBY per key, capture each counter future
        val futures = batch
          .groupBy(_._1)
          .map { case (id, elems) =>
            val finalKey = s"0_$id".getBytes(StandardCharsets.UTF_8)
            val countKey = s"cnt:0:$id".getBytes(StandardCharsets.UTF_8)

            // write each slot directly into the final blob
            elems.foreach { case (_, (idx, dbl)) =>
              asyncCmds.setrange(finalKey, idx * 8L, doubleBytes(dbl))
            }

            // bump the counter and capture its future
            val incrFut = asyncCmds.incrby(countKey, elems.size)
            (id, finalKey, countKey, incrFut)
          }
          .toList

        // flush all SETRANGE + INCRBY in a single network round-trip
        asyncCmds.flushCommands()

        // 5b) For each key whose counter ≥ neuronCount, pipeline EXPIRE + DEL
        futures.foreach { case (id, finalKey, countKey, incrFut) =>
          val cnt = incrFut.get()   // blocks until this counter result arrives
          if (cnt >= neuronCount) {
            // set a 20s TTL on the final blob
            asyncCmds.expire(finalKey, 20)
            // delete the counter
            asyncCmds.del(countKey)
            completed += id
          }
        }

        // flush EXPIRE + DEL in one go
        asyncCmds.flushCommands()
        completed.toList
      }(redisDispatcher)
    }
    .mapConcat(identity)
    .map(id => new ProducerRecord[String, String](outputTopic, id, id))
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { _ =>
      // clean up
      asyncConn.close()
      syncConn.close()
      client.shutdown()
      system.terminate()
    }
}
