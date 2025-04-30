package app

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream._
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.lettuce.core.RedisClient
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import scala.concurrent._


object AkkaLayer0 extends App {
  implicit val system: ActorSystem   = ActorSystem("streams-0-opt")
  implicit val mat:    Materializer  = ActorMaterializer()
  implicit val ec:     ExecutionContext = system.dispatcher

  val neuronCount     = 128
  val maxInFlight     = 1024

  // Semaphore for Redis concurrency
  private val sem = new java.util.concurrent.Semaphore(maxInFlight)
  private val blockingDispatcher = system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
  private def withPermit[T](f: => Future[T]): Future[T] =
    Future(sem.acquire())(blockingDispatcher)
      .flatMap(_ => f.transform { res => sem.release(); res })(ec)

  // Zero-GC ByteBuffer per thread
  private val tlBuffer = ThreadLocal.withInitial[ByteBuffer](() =>
    ByteBuffer.allocateDirect(neuronCount * java.lang.Double.BYTES)
      .order(ByteOrder.LITTLE_ENDIAN)
  )
  private def serialize(arr: Array[Double]): Array[Byte] = {
    val buf = tlBuffer.get()
    buf.clear()
    arr.foreach(buf.putDouble)
    buf.flip()
    val out = new Array[Byte](buf.remaining()); buf.get(out); out
  }

  // Redis async commands
  val redisClient = RedisClient.create("redis://localhost:6379")
  val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] =
    redisClient.connect(new ByteArrayCodec()).async()
  private def writeRedis(id: String, data: Array[Byte]): Future[Unit] = {
    val key = s"0_$id".getBytes(StandardCharsets.UTF_8)
    val redisFuture = asyncCmds.setex(key, 20, data)
    val p = Promise[Unit]()
    redisFuture.thenAccept(_ => p.success(()))
      .exceptionally(ex => { p.failure(ex); null })
    p.future
  }

  // Aggregation + offset-batching stage
  val aggregateStage =
    Flow[(String, (Int, Double), ConsumerMessage.CommittableOffset)]
      .statefulMapConcat { () =>
        val buf    = mutable.Map.empty[String, Array[Double]]
        val counts = mutable.Map.empty[String, Int]
        val offsets = mutable.Map.empty[String, ConsumerMessage.CommittableOffsetBatch]
        elem => {
          val (id, (nid, v), offset) = elem
          val arr = buf.getOrElseUpdate(id, Array.ofDim[Double](neuronCount))
          arr(nid) = v
          val cnt = counts.getOrElse(id, 0) + 1
          counts.update(id, cnt)
          val batch = offsets.get(id) match {
            case Some(b) => b.updated(offset)
            case None    => ConsumerMessage.CommittableOffsetBatch.empty.updated(offset)
          }
          offsets.update(id, batch)

          if (cnt == neuronCount) {
            buf.remove(id); counts.remove(id); offsets.remove(id)
            List((id, arr, batch))
          } else Nil
        }
      }

  // Kafka consumer & producer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   "10000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,    "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,  "100")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,          "10")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,         "512000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,   "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,               "1")

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    .map { msg =>
      val Array(nStr, vStr) = msg.record.value.split("\\|", 2)
      (msg.record.key, (nStr.toInt, vStr.toDouble), msg.committableOffset)
    }
    .via(aggregateStage)
    .mapAsyncUnordered(64) { case (id, arr, batch) =>
      withPermit(writeRedis(id, serialize(arr))).map(_ => (id, batch))(ec)
    }
    .map { case (id, batch) =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("layer-1", id, id),
        batch
      )
    }
    .via(Producer.flexiFlow(producerSettings))
    .mapAsync(parallelism = 4)(_.passThrough.commitScaladsl())
    .runWith(Sink.ignore)
    .onComplete { _ =>
      redisClient.shutdown(); system.terminate()
    }
}
