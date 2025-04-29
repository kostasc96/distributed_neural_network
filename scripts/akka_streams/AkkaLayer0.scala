package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  implicit val system: ActorSystem = ActorSystem("streams-0-opt")
  // Increase per-stage buffer sizes
  private val matSettings = ActorMaterializerSettings(system)
    .withInputBuffer(initialSize = 64, maxSize = 64)
  implicit val mat: ActorMaterializer = ActorMaterializer(matSettings)
  implicit val ec: ExecutionContext = system.dispatcher

  // Constants
  val neuronCount      = 128
  val redisParallelism = 128

  // Thread-local ByteBuffer for efficient serialization
  private val tlBuffer = ThreadLocal.withInitial[ByteBuffer](() =>
    ByteBuffer.allocateDirect(neuronCount * java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
  )
  private def serialize(arr: Array[Double]): Array[Byte] = {
    val buf = tlBuffer.get()
    buf.clear()
    arr.foreach(buf.putDouble)
    buf.flip()
    val out = new Array[Byte](buf.remaining())
    buf.get(out)
    out
  }

  // Redis client and connection pool of StatefulRedisConnection
  val redisClient = RedisClient.create("redis://localhost:6379")
  val poolConfig = new GenericObjectPoolConfig[StatefulRedisConnection[Array[Byte], Array[Byte]]]()
  poolConfig.setMaxTotal(redisParallelism)
  private val connectionPool = ConnectionPoolSupport.createGenericObjectPool(
    () => redisClient.connect(new ByteArrayCodec()),
    poolConfig
  )

  // Aggregation: collect 128 neuron values per key
  case class AggState(arr: Array[Double], var count: Int)
  val aggregateStage = Flow[(String, (Int, Double))]
    .statefulMapConcat { () =>
      val state = mutable.Map.empty[String, AggState]
      elem => {
        val (id, (nid, v)) = elem
        val st = state.getOrElseUpdate(id, AggState(Array.ofDim[Double](neuronCount), 0))
        st.arr(nid) = v
        st.count += 1
        if (st.count == neuronCount) {
          state.remove(id)
          List((id, st.arr))
        } else Nil
      }
    }

  // Kafka consumer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")

  // Kafka producer settings
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")
    .withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

  // Stream definition
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    .map { msg =>
      val value     = msg.value()
      val sepIdx    = value.indexOf('|')
      val neuronIdx = value.substring(0, sepIdx).toInt
      val neuronVal = value.substring(sepIdx + 1).toDouble
      (msg.key(), (neuronIdx, neuronVal))
    }
    .via(aggregateStage)
    .groupedWithin(500, 200.millis)
    .mapAsyncUnordered(redisParallelism) { batch =>
      // Borrow a connection, use pipelined async commands, then return
      val conn: StatefulRedisConnection[Array[Byte], Array[Byte]] = connectionPool.borrowObject()
      try {
        val asyncCmds = conn.async()
        asyncCmds.setAutoFlushCommands(false)
        batch.foreach { case (id, arr) =>
          val key = s"0_$id".getBytes(StandardCharsets.UTF_8)
          asyncCmds.setex(key, 20, serialize(arr))
        }
        asyncCmds.flushCommands()
        Future.successful(batch)
      } finally {
        connectionPool.returnObject(conn)
      }
    }
    .mapConcat(identity)
    .map { case (id, _) =>
      ProducerMessage.single(new ProducerRecord[String, String]("layer-1", id, id))
    }
    .via(Producer.flexiFlow(producerSettings))
    .runWith(Sink.ignore)
    .onComplete { _ =>
      connectionPool.close()
      redisClient.shutdown()
      system.terminate()
    }
}
