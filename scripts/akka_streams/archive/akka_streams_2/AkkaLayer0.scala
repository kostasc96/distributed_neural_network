package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.compat.java8.FutureConverters

// A GraphStage that buffers exactly `neuronCount` outputs per image and emits once full.
class NeuronAggregator(neuronCount: Int)
  extends GraphStage[FlowShape[(String, (Int, Double)), (String, Array[Double])]] {

  val in  = Inlet[(String, (Int, Double))]("NeuronAggregator.in")
  val out = Outlet[(String, Array[Double])]("NeuronAggregator.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    // state: imageId -> (array of outputs, set of seen indices, count)
    private val state = mutable.Map.empty[String, (Array[Double], mutable.BitSet, Int)]

    // Handler for incoming elements
    setHandler(in, new akka.stream.stage.InHandler {
      override def onPush(): Unit = {
        val (imageId, (neuronId, value)) = grab(in)
        val (arr, seen, count) = state.getOrElseUpdate(
          imageId,
          (Array.fill[Double](neuronCount)(0.0), mutable.BitSet.empty, 0)
        )
        if (!seen.contains(neuronId)) {
          arr(neuronId) = value
          seen += neuronId
          state.update(imageId, (arr, seen, count + 1))
        }

        if (state(imageId)._3 >= neuronCount) {
          state -= imageId
          push(out, (imageId, arr))
        } else {
          pull(in)
        }
      }
    })

    setHandler(out, new akka.stream.stage.OutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

object AkkaLayer0 extends App {
  implicit val system: ActorSystem = ActorSystem("streams-0")
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContext = system.dispatcher

  // -- Kafka consumer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-0-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")

  // -- Kafka producer settings
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")

  // -- Lettuce Redis client
  val redisClient = RedisClient.create("redis://localhost:6379")
  val connection = redisClient.connect(new ByteArrayCodec())
  val redisAsync: RedisAsyncCommands[Array[Byte], Array[Byte]] = connection.async()

  val neuronCount = 128

  // Serialize an Array[Double] into a little-endian byte array
  private def serialize(outputs: Array[Double]): Array[Byte] = {
    val buf = ByteBuffer.allocate(outputs.length * java.lang.Double.BYTES)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    outputs.foreach(buf.putDouble)
    buf.array()
  }

  // Asynchronous Redis write using Lettuce
  private def writeToRedis(imageId: String, outputs: Array[Double]): Future[Unit] = {
    val key = s"0_$imageId".getBytes(StandardCharsets.UTF_8)
    val value = serialize(outputs)
    val javaF: RedisFuture[String] = redisAsync.setex(key, 5, value)
    FutureConverters.toScala(javaF).map(_ => ())
  }

  // Build & run the stream
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
    // Parse "neuronId|value" into a tuple of (imageId, (neuronId, value))
    .map { msg =>
      val Array(nStr, vStr) = msg.value().split("\\|", 2)
      (msg.key(), (nStr.toInt, vStr.toDouble))
    }
    // Aggregate 128 neuron outputs before emitting
    .via(new NeuronAggregator(neuronCount))
    // Write to Redis, pass through imageId
    .mapAsyncUnordered(16) { case (imageId, arr) =>
      writeToRedis(imageId, arr).map(_ => imageId)
    }
    // Produce each imageId to the next Kafka topic
    .map { imageId =>
      new ProducerRecord[String, String]("layer-1", imageId, imageId)
    }
    // Send to Kafka and ignore the results
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { _ =>
      connection.close()
      redisClient.shutdown()
      system.terminate()
    }
}
