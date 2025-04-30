package app

import akka.actor.{ActorSystem, Cancellable}
import akka.dispatch.MessageDispatcher
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorAttributes, Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.concurrent._
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  // 1) INLINE CONFIG: stream buffers + dedicated dispatchers
  val customConf = ConfigFactory.parseString("""
    akka.stream.materializer.initial-input-buffer-size = 64
    akka.stream.materializer.max-input-buffer-size     = 256

    akka.actor.redis-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 128 }
      throughput = 1
    }

    akka.actor.kafka-consumer-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 16 }
      throughput = 1
    }
  """).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem   = ActorSystem("streams-0-opt", customConf)
  implicit val mat: Materializer     = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext  = system.dispatcher

  // 2) LOOK UP REDIS DISPATCHER
  val redisDispatcher: MessageDispatcher =
    system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // Constants
  val neuronCount      = 128
  val redisParallelism = 128

  // 3) “Copying” ByteBufferCodec for safe zero-copy
  class CopyingByteBufferCodec() extends RedisCodec[ByteBuffer, ByteBuffer] {
    override def decodeKey(bytes: ByteBuffer): ByteBuffer = {
      val dup = bytes.duplicate()
      val out = ByteBuffer.allocate(dup.remaining())
      out.put(dup).flip()
      out
    }
    override def decodeValue(bytes: ByteBuffer): ByteBuffer = {
      val dup = bytes.duplicate()
      val out = ByteBuffer.allocate(dup.remaining())
      out.put(dup).flip()
      out
    }
    override def encodeKey(key: ByteBuffer): ByteBuffer = {
      val dup = key.duplicate()
      val out = ByteBuffer.allocate(dup.remaining())
      out.put(dup).flip()
      out
    }
    override def encodeValue(value: ByteBuffer): ByteBuffer = {
      val dup = value.duplicate()
      val out = ByteBuffer.allocateDirect(dup.remaining())
      out.put(dup).flip()
      out
    }
  }

  // 4) Redis pool with our CopyingByteBufferCodec
  val redisClient = RedisClient.create("redis://localhost:6379")
  val poolCfg = new GenericObjectPoolConfig[StatefulRedisConnection[ByteBuffer, ByteBuffer]]()
  poolCfg.setMaxTotal(redisParallelism)
  poolCfg.setMaxIdle(redisParallelism)
  poolCfg.setMinIdle(math.min(redisParallelism, 16))
  poolCfg.setBlockWhenExhausted(true)
  poolCfg.setMaxWaitMillis(10000)

  private val connectionPool = ConnectionPoolSupport.createGenericObjectPool(
    () => redisClient.connect(new CopyingByteBufferCodec()),
    poolCfg
  )

  // 5) Thread‐local direct ByteBuffer for serialization input
  private val tlBuffer = ThreadLocal.withInitial[ByteBuffer](() =>
    ByteBuffer.allocateDirect(neuronCount * java.lang.Double.BYTES)
      .order(ByteOrder.LITTLE_ENDIAN)
  )
  private def serializeToBuffer(arr: Array[Double]): ByteBuffer = {
    val buf = tlBuffer.get()
    buf.clear()
    arr.foreach(buf.putDouble)
    buf.flip()
    buf
  }

  // 6) Aggregation stage
  case class AggState(arr: Array[Double], var count: Int)
  val aggregateStage =
    Flow[(String, (Int, Double))].statefulMapConcat { () =>
      val state = new scala.collection.mutable.HashMap[String, AggState]()
      elem =>
        val (id, (nid, v)) = elem
        val st = state.getOrElseUpdate(id, AggState(Array.ofDim[Double](neuronCount), 0))
        st.arr(nid) = v
        st.count += 1
        if (st.count == neuronCount) {
          state.remove(id)
          List((id, st.arr))
        } else Nil
    }

  // 7) Heartbeat
  val heartbeat: Source[(String, Array[Double]), Cancellable] =
    Source.tick(200.millis, 200.millis, ())
      .map(_ => ("__heartbeat__", Array.ofDim[Double](neuronCount)))

  // 8) Kafka settings (consumer pinned by ActorAttributes)
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,      "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        "100")
    .withProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"10485760")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,                       "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,                "2097152")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,            "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                          "1")
    .withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10")

  // 9) Build & run the stream
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
    .map { msg =>
      val v   = msg.value()
      val i   = v.indexOf('|')
      val idx = v.substring(0, i).toInt
      val dbl = v.substring(i + 1).toDouble
      (msg.key(), (idx, dbl))
    }
    .buffer(1024, OverflowStrategy.backpressure)
    .via(aggregateStage)
    .merge(heartbeat)
    .groupedWithin(500, 200.millis)
    .mapAsyncUnordered(redisParallelism) { batch =>
      val real = batch.filterNot(_._1 == "__heartbeat__")
      if (real.isEmpty) Future.successful(Nil)
      else {
        Future {
          val conn = connectionPool.borrowObject()
          try {
            val async = conn.async()
            async.setAutoFlushCommands(false)
            real.foreach { case (id, arr) =>
              val keyBuf  = ByteBuffer.wrap(s"0_$id".getBytes(StandardCharsets.UTF_8))
              val dataBuf = serializeToBuffer(arr)
              async.setex(keyBuf, 20, dataBuf)
            }
            async.flushCommands()
          } finally {
            connectionPool.returnObject(conn)
          }
          real
        }(redisDispatcher)
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
