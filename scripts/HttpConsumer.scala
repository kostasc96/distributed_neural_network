package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


object HttpConsumer extends App {
  implicit val system: ActorSystem = ActorSystem("HighThroughputHttpKafkaConsumer")
  implicit val mat: Materializer = ActorMaterializer()(system)
  import system.dispatcher

  val bootstrapServers = "kafka:9092"

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("high-throughput-http-consumer")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (1024 * 1024).toString)
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")
    .withProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (10 * 1024 * 1024).toString)

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")
    .withProperty(ProducerConfig.RETRIES_CONFIG, "5")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "50")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, (64 * 1024).toString)
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")

  val deadLetterSink = Producer.plainSink(producerSettings)
  val http = Http(system)

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics("requests-responses"))
    .mapAsyncUnordered(16) { msg =>
      val url = msg.record.value()
      println(s"Sending async HTTP request to: $url")

      http.singleRequest(HttpRequest(uri = url)).flatMap { response =>
        response.discardEntityBytes()
        msg.committableOffset.commitScaladsl()
      }.recoverWith { case ex =>
        val errorMessage = s"DLQ - Error for URL: $url | Reason: ${ex.getMessage}"

        Source.single(new ProducerRecord[String, String](
          "dead-letter-topic",
          errorMessage
        )).runWith(deadLetterSink).flatMap(_ => msg.committableOffset.commitScaladsl())
      }
    }
    .runWith(Sink.ignore)
}
