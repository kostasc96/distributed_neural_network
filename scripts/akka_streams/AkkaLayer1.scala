package app

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream._
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import akka.NotUsed

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

object AkkaLayer1 extends App {
  // ——— Akka setup ———
  implicit val system: ActorSystem      = ActorSystem("streams-1-opt")
  implicit val mat:    Materializer            = ActorMaterializer()
  implicit val ec                          = system.dispatcher

  // ——— Business parameters ———
  val neuronCount     = 10
  val parallelism     = 1024

  // ——— Aggregate exactly `neuronCount` per key ———
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

  // ——— Kafka consumer (auto-commit) ———
  val consumerSettings = ConsumerSettings(system,
    new StringDeserializer,
    new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,         "1048576") // 1 MB
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,       "50")      // 50 ms max wait

  // ——— Kafka producer ———
  val producerSettings = ProducerSettings(system,
    new StringSerializer,
    new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,                      "1")      // 1 ms
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,                   "1048576") // 1 MB
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,              "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                         "all")
    .withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5")

  // ——— Stream topology ———
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      val Array(nStr, vStr) = msg.value.split("\\|", 2)
      (msg.key, (nStr.toInt, vStr.toDouble))
    }
    .via(aggregateStage)
    .mapAsyncUnordered(parallelism) { case (id, arr) =>
      // compute max index in parallel
      Future.successful((id, arr.zipWithIndex.maxBy(_._1)._2))
    }
    // wrap into a ProducerMessage so we can use flexiFlow
    .map { case (id, maxIdx) =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("layer-output", id, s"$id|$maxIdx"),
        NotUsed
      )
    }
    // this will push up to `max.in.flight` messages concurrently
    .via(Producer.flexiFlow(producerSettings))
    // drop the envelope, we don't need the passThrough
    .map(_.passThrough)
    .runWith(Sink.ignore)
    .onComplete { _ =>
      system.terminate()
    }
}
