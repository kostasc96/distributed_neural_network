package app

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.stream.{ActorAttributes, KillSwitches, Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.ConfigFactory
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.nio.{ByteBuffer, ByteOrder}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object AkkaLayer0 extends App {
  // === 1) Config & System ===
  val config = ConfigFactory.load()
  implicit val system: ActorSystem       = ActorSystem("streams-intermediate", config)
  implicit val mat: Materializer         = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext      = system.dispatcher
  // additional dispatchers
  val computeDispatcher: ExecutionContext = system.dispatchers.lookup("akka.actor.compute-dispatcher")

  // === 2) Redis async setup ===
  val redisUri    = sys.env("REDIS_URI")
  val neuronCount = config.getInt("akka.stream.neuron-count")
  val redisTtlSec = config.getInt("akka.stream.redis-ttl-sec")
  private val client: RedisClient = RedisClient.create(redisUri)
  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] = client.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] = asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)
  // custom dispatcher for Redis operations
  val redisDispatcher: ExecutionContext = system.dispatchers.lookup("akka.actor.redis-dispatcher")

  def doubleBytes(d: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(d).array()
  }

  // === 3) Kafka settings ===
  val inputTopic   = config.getString("myapp.kafka.input-topic")
  val outputTopic  = config.getString("myapp.kafka.output-topic")
  val kafkaServers = sys.env("KAFKA_BOOTSTRAP_SERVERS")
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaServers)
    .withGroupId("akka-streams0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val producerSettings  = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(kafkaServers)
  val committerSettings = CommitterSettings(system)
    .withMaxBatch(50)
    .withMaxInterval(2.seconds)

  val parallelism = config.getInt("akka.stream.parallelism")
  val bufferSize  = config.getInt("akka.stream.buffer-size")
  val batchSize   = config.getInt("akka.stream.batch-size")
  val batchDur    = config.getString("akka.stream.batch-duration") match {
    case s if s.endsWith("ms") => s.dropRight(2).toInt.millis
    case s if s.endsWith("s")  => s.dropRight(1).toInt.seconds
    case _ => throw new IllegalArgumentException("Invalid batch-duration")
  }

  // === 4) Stream ===
  Consumer.committableSource(consumerSettings, Subscriptions.topics(inputTopic))
    .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
    .viaMat(KillSwitches.single)(Keep.right)
    .map { msg =>
      val v   = msg.record.value()
      val sep = v.indexOf('|')
      (msg.committableOffset, msg.record.key(), v.substring(0, sep).toLong, v.substring(sep + 1).toDouble)
    }
    .buffer(bufferSize, OverflowStrategy.backpressure)
    .groupedWithin(batchSize, batchDur)
    .mapAsyncUnordered(parallelism) { batch =>
      Future {
        import scala.collection.mutable.ListBuffer
        // Prepare Redis pipeline
        asyncCmds.setAutoFlushCommands(false)

        // Track (id, lastOffset, incrFuture)
        val updates = batch
          .groupBy(_._2)
          .map { case (id, elems) =>
            val dataKey  = s"0_$id".getBytes
            val countKey = s"cnt:0:$id".getBytes
            elems.foreach { case (_, _, idx, dbl) =>
              asyncCmds.setrange(dataKey, idx * 8L, doubleBytes(dbl))
            }
            val incrF = asyncCmds.incrby(countKey, elems.size)
            // Return tuple of id, last offset, and a tuple of keys/future
            (id, elems.last._1, (dataKey, countKey, incrF))
          }
          .toList

        // Flush all commands at once
        asyncCmds.flushCommands()

        // Collect completed ids
        val completed = ListBuffer.empty[(String, CommittableOffset)]
        updates.foreach { case (id, offset, data) =>
          val (dataKey, countKey, incrF) = data.asInstanceOf[(Array[Byte], Array[Byte], io.lettuce.core.RedisFuture[java.lang.Long])]
          if (incrF.get() >= neuronCount) {
            asyncCmds.expire(dataKey, redisTtlSec)
            asyncCmds.del(countKey)
            completed += ((id, offset))
          }
        }
        // Return list of completed (id, offset)
        completed.toList
      }(redisDispatcher)
    }
    .mapConcat(identity)
    .map { case (id, offset) =>
      // emit to Kafka
      ProducerMessage.single(new ProducerRecord[String, String](outputTopic, id, id), offset)
    }
    .via(Producer.flexiFlow(producerSettings)) // you can attach a producer dispatcher if desired (e.g. .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-producer-dispatcher")))
    .map(_.passThrough)
    .toMat(Committer.sink(committerSettings))(Keep.both)
    .run()

  // === 5) Shutdown hook ===
  sys.addShutdownHook {
    asyncConn.close()
    client.shutdown()
    system.terminate()
  }
}
