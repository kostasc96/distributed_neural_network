package app

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, SystemMaterializer}
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.ExecutionContext

object AkkaLayer1 extends App {
  // ——— Akka setup ———
  val customConf = ConfigFactory.parseString("""
akka.stream.materializer.initial-input-buffer-size = 64
akka.stream.materializer.max-input-buffer-size = 256
""")
  implicit val system: ActorSystem = ActorSystem("streams-0-opt", ConfigFactory.load(customConf))
  implicit val mat: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.dispatcher

  // ——— Business parameters ———
  val neuronCount = 10

  // ——— Aggregation stage: collect exactly `neuronCount` values per key ———
  val aggregateStage: Flow[(String, (Int, Double)), (String, Array[Double]), NotUsed] =
    Flow[(String, (Int, Double))].statefulMapConcat { () =>
      val buf    = mutable.Map.empty[String, Array[Double]]
      val counts = mutable.Map.empty[String, Int]
      elem =>
        val (id, (idx, v)) = elem
        val arr = buf.getOrElseUpdate(id, Array.ofDim[Double](neuronCount))
        arr(idx) = v
        val cnt = counts.getOrElse(id, 0) + 1
        counts.update(id, cnt)
        if (cnt == neuronCount) {
          buf.remove(id); counts.remove(id)
          List((id, arr))
        } else Nil
    }

  // ——— Heartbeat source to avoid starvation ———
  val heartbeat: Source[(String, Array[Double]), Cancellable] =
    Source.tick(200.millis, 200.millis, ())
      .map(_ => ("__heartbeat__", Array.ofDim[Double](neuronCount)))

  // ——— Kafka consumer settings ———
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,         "1048576") // 1 MB
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,       "50")      // 50 ms

  // ——— Kafka producer settings ———
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,                        "10")     // 1 ms
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,                   "1048576") // 1 MB
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,              "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                         "all")
    .withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5")

  // ——— Stream topology with heartbeat merge ———
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      // Parse Kafka message value "n|v" into (neuronIndex, value)
      val Array(nStr, vStr) = msg.value.split("\\|", 2)
      (msg.key, (nStr.toInt, vStr.toDouble))
    }
    .via(aggregateStage)
    .async                          // introduce async boundary after aggregation
    .merge(heartbeat)
    .map {
      case ("__heartbeat__", _) => None
      case (id, arr) =>
        // Find index of max value in the array
        val maxIdx = arr.indices.maxBy(i => arr(i))
        Some(new ProducerRecord[String, String]("layer-output", id, s"$id|$maxIdx"))
    }
    .collect { case Some(record) => record }
    .async                          // separate producer stage
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { _ =>
      system.terminate()
    }
}
