package app

import akka.actor.{ActorSystem, Cancellable}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
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
import scala.concurrent._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  val customConf = ConfigFactory.parseString("""
akka.stream.materializer.initial-input-buffer-size = 64
akka.stream.materializer.max-input-buffer-size = 256
""")
  implicit val system: ActorSystem = ActorSystem("streams-0-opt", ConfigFactory.load(customConf))
  implicit val mat: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.dispatcher

  // Constants
  val neuronCount      = 128
  val redisParallelism = 128

  // Thread-local ByteBuffer for efficient serialization (avoid heap allocation)
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

  // Redis connection pool configuration
  val redisClient = RedisClient.create("redis://localhost:6379")
  val poolConfig = new GenericObjectPoolConfig[StatefulRedisConnection[Array[Byte], Array[Byte]]]()
  poolConfig.setMaxTotal(redisParallelism)
  poolConfig.setMaxIdle(redisParallelism)
  poolConfig.setMinIdle(math.min(redisParallelism, 16))
  poolConfig.setBlockWhenExhausted(true)
  poolConfig.setMaxWaitMillis(10000)  // wait up to 10s for a connection

  private val connectionPool = ConnectionPoolSupport.createGenericObjectPool(
    () => redisClient.connect(new ByteArrayCodec()),
    poolConfig
  )

  // Aggregation: collect 128 neuron values per key (id)
  case class AggState(arr: Array[Double], var count: Int)
  val aggregateStage = Flow[(String, (Int, Double))]
    .statefulMapConcat { () =>
      val state = scala.collection.mutable.Map.empty[String, AggState]
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

  // Heartbeat source to flush groups periodically even if no new data
  val heartbeat: Source[(String, Array[Double]), Cancellable] =
    Source.tick(200.millis, 200.millis, ()).map(_ => ("__heartbeat__", Array.ofDim[Double](neuronCount)))

  // Kafka consumer and producer settings tuned for throughput
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576") // 1 MB
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")
    .withProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760") // 10 MB

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "10")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, "2097152") // 2 MB
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")
    .withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10")

  // Stream definition
  Consumer.plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    .map { msg =>
      // Parse key|value into (id, (neuronIndex, neuronValue))
      val value = msg.value()
      val sepIdx = value.indexOf('|')
      val neuronIdx = value.substring(0, sepIdx).toInt
      val neuronVal = value.substring(sepIdx + 1).toDouble
      (msg.key(), (neuronIdx, neuronVal))
    }
    .buffer(1024, OverflowStrategy.backpressure)
    .via(aggregateStage)
    .merge(heartbeat)
    .groupedWithin(500, 200.millis)
    .mapAsyncUnordered(redisParallelism) { batch =>
      // Filter out heartbeat entries
      val realBatch = batch.filterNot(_._1 == "__heartbeat__")
      if (realBatch.isEmpty) {
        Future.successful(Nil)
      } else {
        // Perform Redis writes in Future to avoid blocking stream dispatcher
        Future {
          val conn = connectionPool.borrowObject()
          try {
            val asyncCmds = conn.async()
            asyncCmds.setAutoFlushCommands(false)
            realBatch.foreach { case (id, arr) =>
              val key = s"0_$id".getBytes(StandardCharsets.UTF_8)
              asyncCmds.setex(key, 20, serialize(arr))
            }
            asyncCmds.flushCommands()
            realBatch
          } finally {
            connectionPool.returnObject(conn)
          }
        }
      }
    }
    .mapConcat(identity)
    .map { case (id, _) =>
      new ProducerRecord[String, String]("layer-1", id, id)
    }
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { _ =>
      connectionPool.close()
      redisClient.shutdown()
      system.terminate()
    }
}
