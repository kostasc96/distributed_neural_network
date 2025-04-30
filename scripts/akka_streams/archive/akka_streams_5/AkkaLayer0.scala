package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import io.lettuce.core.RedisClient
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.codec.ByteArrayCodec
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Semaphore

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  implicit val system:              ActorSystem   = ActorSystem("streams-0")
  implicit val materializer:        Materializer  = ActorMaterializer()(system)
  implicit val ec:                  ExecutionContext = system.dispatcher

  val neuronCount  = 128
  val maxKeysInFlight = 1024
  private val keyPermit = new Semaphore(maxKeysInFlight)

  /** Wraps a Future so that at most `maxKeysInFlight` of them can be
   * running at the same time. */
  private def withPermit[A](fut: => Future[A]): Future[A] =
    Future(blocking(keyPermit.acquire()))     // block (back-pressure) if no permits
      .flatMap(_ => fut.transform { result =>
        keyPermit.release()
        result
      })

  // zero-GC serialization buffer
  private val tlBuffer = ThreadLocal.withInitial[ByteBuffer](() =>
    ByteBuffer.allocateDirect(neuronCount * java.lang.Double.BYTES)
      .order(ByteOrder.LITTLE_ENDIAN)
  )

  private def serialize(outputs: Array[Double]): Array[Byte] = {
    val buf = tlBuffer.get()
    buf.clear()
    outputs.foreach(buf.putDouble)
    buf.flip()
    val arr = new Array[Byte](buf.remaining())
    buf.get(arr)
    arr
  }

  // Redis client
  val redisClient = RedisClient.create("redis://localhost:6379")
  val connection  = redisClient.connect(new ByteArrayCodec())
  val redisReactive: RedisReactiveCommands[Array[Byte], Array[Byte]] =
    connection.reactive()

  private def writeReactive(id: String, data: Array[Byte]): Future[String] = {
    val key = s"0_$id".getBytes(StandardCharsets.UTF_8)
    toScala(redisReactive.setex(key, 20, data).toFuture)
  }

  // 1) Unlimited groupBy so we never hit a SubstreamLimit
  val aggregatorFlow = Flow[(String, (Int, Double))]
    .groupBy(Int.MaxValue, _._1)
    .groupedWithin(neuronCount, 60.seconds)
    .filter(_.size == neuronCount)
    .map { batch =>
      val id  = batch.head._1
      val arr = Array.fill[Double](neuronCount)(0.0)
      batch.foreach { case (_, (nid, v)) => arr(nid) = v }
      (id, arr)
    }
    .mergeSubstreams

  // Kafka settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,     "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,   "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,    "5000")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,           "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,          "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                "1")

  // Build & run: we only allow 1024 keys in flight because of our semaphore
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    .map { msg =>
      val Array(nStr, vStr) = msg.value.split("\\|", 2)
      (msg.key, (nStr.toInt, vStr.toDouble))
    }
    .via(aggregatorFlow)
    .mapAsyncUnordered(64) { case (id, arr) =>
      // throttle “new” id‐streams to 1024 permits
      withPermit {
        writeReactive(id, serialize(arr)).map(_ => id)
      }
    }
    .map(id => new ProducerRecord[String, String]("layer-1", id, id))
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { _ =>
      connection.close()
      redisClient.shutdown()
      system.terminate()
    }
}
