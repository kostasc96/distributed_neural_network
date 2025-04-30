package app

import java.util.concurrent.TimeUnit
import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, Cancellable}
import akka.event.Logging
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.{AccessStyle, MemoryBufferType, S3Attributes, S3Settings}
import akka.stream.scaladsl._
import akka.util.ByteString
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => SdkRegion}
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object AkkaLayer1 extends App {
  // — ActorSystem & Materializer —
  implicit val system: ActorSystem      = ActorSystem("streams-s3")
  implicit val ec: ExecutionContext     = system.dispatcher
  implicit val mat: Materializer        = Materializer(system)
  private val log = Logging(system, getClass)

  // — Business settings —
  val neuronCount = 10
  val ttl         = 5.minutes

  // — Caffeine cache for TTL eviction —
  private val cache = Caffeine.newBuilder()
    .expireAfterWrite(ttl.toMillis, TimeUnit.MILLISECONDS)
    .build[String, (Array[Double], Int)]()

  // — 1) Aggregation stage using Caffeine —
  val aggregateStage: Flow[(String,(Int,Double)), (String,Array[Double]), NotUsed] =
    Flow[(String,(Int,Double))].mapConcat { case (id,(idx,v)) =>
      val (arr, cnt) = Option(cache.getIfPresent(id)).getOrElse {
        val newArr = Array.ofDim[Double](neuronCount)
        cache.put(id, (newArr, 0))
        (newArr, 0)
      }
      arr(idx) = v
      val newCnt = cnt + 1
      cache.put(id, (arr, newCnt))
      if (newCnt == neuronCount) {
        cache.invalidate(id)
        List((id, arr))
      } else Nil
    }

  // — 2) Heartbeat to keep stream alive —
  val heartbeat: Source[(String,Array[Double]), Cancellable] =
    Source.tick(200.millis, 200.millis, ())
      .map(_ => ("__heartbeat__", Array.ofDim[Double](neuronCount)))

  // — 3) Kafka settings (auto-commit enabled) —
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-opt")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        "50000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,         "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,       "50")

  // — 4) Minio/S3 config & credentials —
  val bucket          = "faasnn-bucket"
  val accessKeyId     = "admin"
  val secretAccessKey = "admin123"
  val endpointUrl     = "http://localhost:9000"

  val creds = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
  S3AsyncClient.builder()
    .credentialsProvider(StaticCredentialsProvider.create(creds))
    .endpointOverride(new java.net.URI(endpointUrl))
    .region(SdkRegion.EU_WEST_1)
    .build()

  implicit val s3Settings: S3Settings =
    S3Settings(system)
      .withCredentialsProvider(StaticCredentialsProvider.create(creds))
      .withS3RegionProvider(new AwsRegionProvider {
        override def getRegion: SdkRegion = SdkRegion.EU_WEST_1
      })
      .withEndpointUrl(endpointUrl)
      .withAccessStyle(AccessStyle.PathAccessStyle)
      .withBufferType(MemoryBufferType)

  // — 5) Source: Kafka → aggregate → heartbeat → ByteString CSV lines —
  val source: Source[ByteString, Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
      .map { msg =>
        val Array(nStr,vStr) = msg.value.split("\\|",2)
        (msg.key, (nStr.toInt, vStr.toDouble))
      }
      .via(aggregateStage)
      .merge(heartbeat)
      .collect { case (id, arr) if id != "__heartbeat__" =>
        ByteString(s"$id,${arr.indices.maxBy(i => arr(i))}\n")
      }

  // — 6) Sink: chunk → prepend header → parallel multipart uploads (error-only logging) —
  val parallelism = 4
  val keyPrefix   = "predictions/part"

  val done: Future[Done] = source
    .groupedWithin(500, 3.minutes)    // Seq[ByteString]
    .zipWithIndex                      // (Seq, idx)
    .mapAsyncUnordered(parallelism) { case (chunk, idx) =>
      val key  = f"$keyPrefix-${idx + 1}%05d.csv"
      val header = ByteString("image_i,prediction\n")
      val rows   = Source.single(header) ++ Source(chunk.toList)
      val sink   = S3.multipartUpload(bucket, key)
        .withAttributes(S3Attributes.settings(s3Settings))

      rows
        .runWith(sink)
        .map(_ => Done)
        .andThen {
          case Failure(ex) =>
            log.error(ex, s"Upload FAILED: $key")
        }
    }
    .runWith(Sink.ignore)

  // — Shutdown on completion or failure —
  done.onComplete {
    case scala.util.Success(_)  =>
      log.info("All uploads complete, shutting down.")
      system.terminate()
    case Failure(ex) =>
      log.error(ex, "Stream failed, shutting down.")
      system.terminate()
  }
}
