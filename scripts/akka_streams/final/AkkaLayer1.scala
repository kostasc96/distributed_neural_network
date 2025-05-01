package app

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import scala.util.Random

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.event.Logging
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.{ActorAttributes, Materializer, OverflowStrategy, SystemMaterializer}
import akka.stream.alpakka.s3.{AccessStyle, MemoryBufferType, MultipartUploadResult, S3Attributes, S3Settings}
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl._
import akka.util.ByteString

import com.typesafe.config.ConfigFactory

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => SdkRegion}
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AkkaLayer1 extends App {
  // 1) CONFIG: add redis dispatcher
  val customConf = ConfigFactory.parseString("""
    akka.actor.kafka-consumer-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 16 }
      throughput = 1
    }
    akka.actor.s3-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 8 }
      throughput = 1
    }
    akka.actor.redis-dispatcher {
      type = Dispatcher
      executor = "thread-pool-executor"
      thread-pool-executor { fixed-pool-size = 8 }
      throughput = 1
    }
  """).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem      = ActorSystem("streams-s3-redis", customConf)
  implicit val mat: Materializer        = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext     = system.dispatcher
  private val log = Logging(system, getClass)

  // dedicated execution contexts
  private val kafkaEc: ExecutionContext = system.dispatchers.lookup("akka.actor.kafka-consumer-dispatcher")
  private val s3Ec:   ExecutionContext  = system.dispatchers.lookup("akka.actor.s3-dispatcher")
  private val redisEc: ExecutionContext = system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // 2) BUSINESS SETTINGS
  val neuronCount = 10
  val ttl         = 5.minutes

  // 3) REDIS: async+pipelined + sync for incrby futures
  private val client = RedisClient.create("redis://localhost:6379")

  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    client.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] = asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)

  private val syncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    client.connect(ByteArrayCodec.INSTANCE)
  private val syncCmds = syncConn.sync()

  // 4) KAFKA CONSUMER
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("akka-streams1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,      "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,     "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000")

  // 5) S3 / MINIO
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
      .withS3RegionProvider(new AwsRegionProvider { override def getRegion: SdkRegion = SdkRegion.EU_WEST_1 })
      .withEndpointUrl(endpointUrl)
      .withAccessStyle(AccessStyle.PathAccessStyle)
      .withBufferType(MemoryBufferType)

  // helper to generate a timestamp+random suffix key
  private val tsFmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'").withZone(ZoneOffset.UTC)
  private def makeKey(): String = {
    val ts = tsFmt.format(Instant.now())
    val suffix = Random.alphanumeric.take(4).mkString
    s"predictions/part-$ts-$suffix.csv"
  }

  // 6) STREAM GRAPH: Kafka → Redis-backed aggregation → S3 upload
  val source: Source[(String,(Int,Double)), Consumer.Control] =
    Consumer
      .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
      .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
      .mapAsync(1) { msg =>
        Future {
          val Array(nStr, vStr) = msg.value.split("\\|",2)
          (msg.key(), (nStr.toInt, vStr.toDouble))
        }(kafkaEc)
      }

  val aggregator: Flow[(String,(Int,Double)), ByteString, NotUsed] =
    Flow[(String,(Int,Double))]
      .buffer(1024, OverflowStrategy.backpressure)
      .groupedWithin(500, 200.millis)
      .mapAsyncUnordered(neuronCount) { batch =>
        Future {
          val completed = ListBuffer.empty[String]
          val ttlSec    = ttl.toSeconds.toInt

          // pipeline SETRANGE, INCRBY, EXPIRE for each id
          val incrFuts = batch
            .groupBy(_._1)
            .map { case (id, elems) =>
              val blobKey  = s"1_$id".getBytes(StandardCharsets.UTF_8)
              val countKey = s"cnt:1:$id".getBytes(StandardCharsets.UTF_8)

              elems.foreach { case (_, (idx, dbl)) =>
                asyncCmds.setrange(blobKey, idx*8L, {
                  val buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                  buf.putDouble(dbl).array()
                })
              }
              val f = asyncCmds.incrby(countKey, elems.size)
              asyncCmds.expire(blobKey, ttlSec)
              asyncCmds.expire(countKey, ttlSec)

              (id, blobKey, countKey, f)
            }.toList

          asyncCmds.flushCommands()

          // collect completed, fetch & compute, delete counter
          incrFuts.foreach { case (id, blobKey, countKey, fut) =>
            if (fut.get() >= neuronCount) {
              val raw = syncCmds.get(blobKey)
              val buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
              val arr = Array.ofDim[Double](neuronCount)
              var i = 0; while (i < neuronCount) { arr(i) = buf.getDouble(); i += 1 }
              val pred = arr.zipWithIndex.maxBy(_._1)._2

              asyncCmds.del(countKey)
              completed += s"$id,$pred\n"
            }
          }

          asyncCmds.flushCommands()
          completed.toList
        }(redisEc)
      }
      .mapConcat(identity _)
      .map(ByteString(_))

  val uploads: Source[MultipartUploadResult, NotUsed] =
    source.via(aggregator)
      .groupedWithin(500, 3.minutes)
      .mapAsyncUnordered(4) { chunk =>
        Future {
          val key    = makeKey()
          val header = ByteString("image_id,prediction\n")
          val rows   = Source.single(header) ++ Source(chunk.toList)
          rows.runWith(
            S3.multipartUpload(bucket, key)
              .withAttributes(S3Attributes.settings(s3Settings))
          )
        }(s3Ec).flatMap(identity)(s3Ec)
      }
      .mapMaterializedValue(_ => NotUsed)

  // 7) MATERIALIZE & SHUTDOWN
  val done: Future[Done] = uploads.runWith(Sink.ignore)
  done.onComplete {
    case Success(_) =>
      log.info("All uploads complete, shutting down.")
      asyncConn.close(); syncConn.close(); client.shutdown(); system.terminate()
    case Failure(ex) =>
      log.error(ex, "Stream failed, shutting down.")
      asyncConn.close(); syncConn.close(); client.shutdown(); system.terminate()
  }
}
