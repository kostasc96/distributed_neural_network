package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, blocking}
import java.util.concurrent.Semaphore

object AkkaLayer1 extends App {
  implicit val system:              ActorSystem   = ActorSystem("streams-1")
  implicit val materializer:        Materializer  = ActorMaterializer()(system)
  implicit val ec:                  ExecutionContext = system.dispatcher

  // Kafka consumer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,     "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,   "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,    "5000")

  // Kafka producer settings
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,           "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,          "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,                "1")

  val neuronCount       = 10
  val maxKeysInFlight   = 1024

  // Semaphore to throttle number of keys "in flight" simultaneously
  private val keyPermit = new Semaphore(maxKeysInFlight)

  private def withPermit[A](fut: => Future[A]): Future[A] =
    Future(blocking(keyPermit.acquire()))(ec).flatMap { _ =>
      fut.transform { result =>
        keyPermit.release()
        result
      }(ec)
    }(ec)

  // 1. Aggregator: unlimited concurrent substreams
  val aggregatorFlow: Flow[(String, (Int, Double)), (String, Array[Double]), _] =
    Flow[(String, (Int, Double))]
      .groupBy(Int.MaxValue, _._1)
      .groupedWithin(neuronCount, 60.seconds)
      .filter(_.size == neuronCount)
      .map { batch =>
        val id  = batch.head._1
        val arr = Array.fill[Double](neuronCount)(0.0)
        batch.foreach { case (_, (nid, v)) => arr(nid) = v }
        (id, arr)
      }
      .mergeSubstreams

  // 2. Prediction: pick max neuron index and wrap into a ProducerRecord
  val predictionFlow = Flow[(String, Array[Double])]
    .map { case (imageId, outputs) =>
      val maxIdx  = outputs.zipWithIndex.maxBy(_._1)._2
      val message = s"$imageId|$maxIdx"
      new ProducerRecord[String, String]("layer-output", imageId, message)
    }

  // Build & run the stream, throttling to maxKeysInFlight concurrent keys
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      val Array(idStr, vStr) = msg.value().split("\\|", 2)
      (msg.key(), (idStr.toInt, vStr.toDouble))
    }
    .via(aggregatorFlow)
    .mapAsyncUnordered(maxKeysInFlight) { tuple =>
      withPermit(Future.successful(tuple))
    }
    .async
    .via(predictionFlow)
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { result =>
      println(s"Stream completed: $result")
      system.terminate()
    }
}
