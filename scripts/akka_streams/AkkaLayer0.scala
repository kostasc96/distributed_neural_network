package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.ProducerMessage
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future


object AkkaLayer0 {
  implicit val system: ActorSystem = ActorSystem("neuron-aggregator-system")
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val ec = system.dispatcher

  val neuronCount = 128
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(12) // Increased pool size for higher throughput
  val redisPool = new JedisPool(poolConfig, "localhost", 6379, 5000, null, false)

  case class AggregationBuffer(outputs: Array[Double], received: Array[Boolean], var count: Int)
  val aggregationState = TrieMap.empty[String, AggregationBuffer]

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("neuron-aggregator-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000")
    .withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000")
    .withProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000")
    .withProperty("receive.buffer.bytes", "1048576")
    .withProperty("send.buffer.bytes", "1048576")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")  // ✅ Changed from 'all' to '1'
    .withProperty(ProducerConfig.RETRIES_CONFIG, "0")  // ✅ No retries
    .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000")  // ✅ Lowered from 300000
    .withProperty("socket.keepalive.enable", "true")
    .withProperty("tcp.nodelay", "true")

  def serializeDoubleArray(values: Array[Double]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.length * java.lang.Double.BYTES)
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    values.foreach(buffer.putDouble)
    buffer.array()
  }

  def writeToRedis(imageId: String, outputs: Array[Double]): Future[Unit] = Future {
    val jedis = redisPool.getResource
    try {
      val pipeline = jedis.pipelined()
      pipeline.setex(s"0_$imageId".getBytes(), 5, serializeDoubleArray(outputs))
      pipeline.sync()
    } finally {
      jedis.close()
    }
  }

  def processNeuronOutput(imageId: String, neuronId: Int, output: Double): Option[(String, Array[Double])] = {
    val buffer = aggregationState.getOrElseUpdate(
      imageId,
      AggregationBuffer(Array.fill[Double](neuronCount)(0.0), Array.fill[Boolean](neuronCount)(false), 0)
    )

    if (!buffer.received(neuronId)) {
      buffer.outputs(neuronId) = output
      buffer.received(neuronId) = true
      buffer.count += 1
    }

    if (buffer.count >= neuronCount) {
      aggregationState.remove(imageId)
      Some((imageId, buffer.outputs))
    } else {
      None
    }
  }

  def main(args: Array[String]): Unit = {
    val decider: Supervision.Decider = {
      case ex =>
        println(s"[ERROR] Stream failed with: $ex. Resuming.")
        Supervision.Resume
    }

    val kafkaGraph = Consumer
      .plainSource(consumerSettings, Subscriptions.topics("layer-0-streams"))
      .mapAsyncUnordered(parallelism = 8) { msg =>
        val value = msg.value()
        val sepIdx = value.indexOf('|')
        if (sepIdx == -1) Future.successful(None)
        else {
          val neuronId = value.substring(0, sepIdx).toInt
          val data = value.substring(sepIdx + 1).toDouble
          val imageId = msg.key()

          if (neuronId < 0 || neuronId >= neuronCount)
            Future.successful(None)
          else
            Future.successful(processNeuronOutput(imageId, neuronId, data).map { case (imgId, outputs) =>
              (imgId, outputs)
            })
        }
      }
      .collect { case Some(result) => result }
      .mapAsyncUnordered(parallelism = 8) { case (imageId, outputs) =>
        writeToRedis(imageId, outputs).map(_ => imageId)
      }
      .map { imageId =>
        val record = new ProducerRecord[String, String]("layer-1", imageId, imageId)
        ProducerMessage.Message(record, passThrough = imageId)
      }
      .via(Producer.flexiFlow(producerSettings))
      .map {
        case ProducerMessage.Result(_, imageId) =>
          imageId
        case other =>
          throw new RuntimeException(s"Unexpected producer result: $other")
      }
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .toMat(Sink.ignore)(Keep.right)

    val done = kafkaGraph.run()

    done.onComplete {
      case scala.util.Success(_) =>
        system.terminate()
      case scala.util.Failure(ex) =>
        println(s"[ERROR] Aggregator stream failed: $ex")
        system.terminate()
    }
  }
}