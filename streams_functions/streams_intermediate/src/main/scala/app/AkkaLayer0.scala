package app

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ProducerMessage
import akka.stream.{ActorAttributes, KillSwitches, Materializer, SystemMaterializer}
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.config.ConfigFactory
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.RedisFuture
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import java.nio.{ByteBuffer, ByteOrder}

object AkkaLayer0 extends App {
  // === 1) Config & System ===
  val config = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("streams-intermediate", config)
  implicit val mat: Materializer   = SystemMaterializer(system).materializer

  // Use dedicated dispatchers
  implicit val computeDispatcher: ExecutionContext =
    system.dispatchers.lookup("akka.actor.compute-dispatcher")
  val redisDispatcher: ExecutionContext =
    system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // === 2) Redis + Lua script setup ===
  val redisUri    = sys.env("REDIS_URI")
  val neuronCount = config.getInt("akka.stream.neuron-count")
  val redisTtlSec = config.getInt("akka.stream.redis-ttl-sec")
  val client: RedisClient = RedisClient.create(redisUri)
  val asyncConn: RedisAsyncCommands[Array[Byte], Array[Byte]] =
    client.connect(ByteArrayCodec.INSTANCE).async()

  // Convert Lettuce RedisFuture to Scala Future
  private def toScala[T](rf: RedisFuture[T]): Future[T] = {
    val p = Promise[T]()
    rf.whenComplete(new java.util.function.BiConsumer[T, Throwable] {
      override def accept(res: T, err: Throwable): Unit =
        if (err != null) p.failure(err) else p.success(res)
    })
    p.future
  }

  // Lua for atomic SETRANGE + INCRBY + cleanup
  private val luaScript =
    s"""
       |local dataKey = KEYS[1]
       |local cntKey  = KEYS[2]
       |local offset  = tonumber(ARGV[1])
       |local byteStr = ARGV[2]
       |local thresh  = tonumber(ARGV[3])
       |redis.call("SETRANGE", dataKey, offset, byteStr)
       |local cnt = redis.call("INCRBY", cntKey, 1)
       |if cnt >= thresh then
       |  redis.call("EXPIRE", dataKey, "$redisTtlSec")
       |  redis.call("DEL", cntKey)
       |  return 1
       |else
       |  return 0
       |end
       |""".stripMargin

  // Load script SHA
  private val scriptSha: String =
    Await.result(toScala(asyncConn.scriptLoad(luaScript)), 30.seconds)

  // Convert Double to little-endian bytes
  private def doubleBytes(d: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(d).array()
  }

  // Atomic Redis update
  private def updateStateAsync(
                                id: String,
                                offset: Long,
                                bytes: Array[Byte]
                              ): Future[Boolean] = {
    val dataKey = s"0_$id".getBytes
    val cntKey  = s"cnt:0:$id".getBytes
    val rf: RedisFuture[java.lang.Long] = asyncConn.evalsha(
      scriptSha,
      ScriptOutputType.INTEGER,
      Array(dataKey, cntKey),
      offset.toString.getBytes,
      bytes,
      neuronCount.toString.getBytes
    )
    toScala(rf).map(_ == 1L)(redisDispatcher)
  }

  // === 3) Kafka settings ===
  val inputTopic   = config.getString("myapp.kafka.input-topic")
  val outputTopic  = config.getString("myapp.kafka.output-topic")
  val bootstrap    = sys.env("KAFKA_BOOTSTRAP_SERVERS")

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrap)
    .withGroupId("akka-streams0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrap)

  val committerSettings = CommitterSettings(system)
    .withMaxBatch(50)
    .withMaxInterval(2.seconds)

  // === 4) Stream with micro-batch and KillSwitch ===
  val (killSwitch, streamDone) = Consumer
    .committableSource(consumerSettings, Subscriptions.topics(inputTopic))
    .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
    .viaMat(KillSwitches.single)(Keep.right)
    .groupedWithin(50, 100.milliseconds)
    .mapAsyncUnordered(config.getInt("akka.stream.parallelism")) { batch =>
      Future.traverse(batch) { msg =>
        val v     = msg.record.value()
        val sep   = v.indexOf('|')
        val id    = msg.record.key()
        val idx   = v.substring(0, sep).toLong
        val dbl   = v.substring(sep + 1).toDouble
        val bytes = doubleBytes(dbl)
        updateStateAsync(id, idx * 8L, bytes).map {
          case true  => Some(
            ProducerMessage.single(
              new ProducerRecord[String, String](outputTopic, id, id),
              msg.committableOffset
            )
          )
          case false => None
        }
      }.map(_.flatten)
    }
    .mapConcat(identity)
    .via(
      Producer.flexiFlow(producerSettings)
        .withAttributes(ActorAttributes.dispatcher("akka.actor.compute-dispatcher"))
    )
    .map {
      case res: ProducerMessage.Result[_, _, CommittableOffset] => res.passThrough
      case other => throw new IllegalStateException(s"Unexpected envelope: $other")
    }
    .toMat(Committer.sink(committerSettings))(Keep.both)
    .run()

  // === 5) CoordinatedShutdown task: drain -> commit -> terminate ===
  CoordinatedShutdown(system).addTask(
    CoordinatedShutdown.PhaseServiceRequestsDone,
    "drain-and-commit-stream"
  ) { () =>
    // stop pulling new messages
    killSwitch.shutdown()
    // wait until all in-flight messages are committed
    streamDone.map { _ =>
      system.log.info("Stream drained and all offsets committed.")
      Done
    }(computeDispatcher)
  }

  // === 6) JVM shutdown hook -> trigger CoordinatedShutdown ===
  sys.addShutdownHook {
    asyncConn.getStatefulConnection.close()
    client.shutdown()
    CoordinatedShutdown(system).run(CoordinatedShutdown.JvmExitReason)
  }
}
