package app

import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import scala.util.Random
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
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
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.{Region => SdkRegion}
import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object AkkaLayer1 extends App {
  // ---- 1) Environment variables helper ----
  def env(name: String): String =
    sys.env.getOrElse(name, throw new IllegalArgumentException(s"Env var '$name' not set"))

  // ---- 2) Load static settings from application.conf ----
  val config = ConfigFactory.load()
  val neuronCount = config.getInt("akka.stream.neuron-count")
  val ttl          = config.getDuration("akka.stream.redis-ttl-sec").toMillis.millis
  val bucket       = config.getString("app.s3.bucket")

  // ---- 3) Read dynamic endpoints from ENV ----
  val kafkaBootstrap = env("KAFKA_BOOTSTRAP_SERVERS")
  val redisUri       = env("REDIS_URI")
  val s3Endpoint     = env("S3_ENDPOINT_URL")
  val s3Region       = env("S3_REGION")
  val s3AccessKey    = env("S3_ACCESS_KEY_ID")
  val s3SecretKey    = env("S3_SECRET_ACCESS_KEY")

  // ---- 4) Actor system & materializer ----
  implicit val system: ActorSystem  = ActorSystem("streams-final", config)
  implicit val mat: Materializer    = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.dispatcher

  // dedicated dispatchers
  private val kafkaEc: ExecutionContext = system.dispatchers.lookup("akka.actor.kafka-consumer-dispatcher")
  private val s3Ec:   ExecutionContext  = system.dispatchers.lookup("akka.actor.s3-dispatcher")
  private val redisEc: ExecutionContext = system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // ---- 5) Redis client setup ----
  private val redisClient = RedisClient.create(redisUri)
  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    redisClient.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] = asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)
  private val syncConn = redisClient.connect(ByteArrayCodec.INSTANCE)
  private val syncCmds = syncConn.sync()

  // ---- 6) Kafka consumer settings ----
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaBootstrap)
    .withGroupId("akka-streams1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,      "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,     "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000")

  // ---- 7) S3 client & Alpakka S3 settings ----
  val awsCreds = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  S3AsyncClient.builder()
    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
    .endpointOverride(URI.create(s3Endpoint))
    .region(SdkRegion.of(s3Region))
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .build()

  implicit val s3Settings: S3Settings =
    S3Settings(system)
      .withCredentialsProvider(StaticCredentialsProvider.create(awsCreds))
      .withS3RegionProvider(() => SdkRegion.of(s3Region))
      .withEndpointUrl(s3Endpoint)
      .withAccessStyle(AccessStyle.PathAccessStyle)
      .withBufferType(MemoryBufferType)

  // ---- 8) Helpers ----
  private val tsFmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'").withZone(ZoneOffset.UTC)
  private def makeKey(): String = {
    val ts     = tsFmt.format(Instant.now())
    val suffix = Random.alphanumeric.take(4).mkString
    s"predictions/part-$ts-$suffix.csv"
  }

  private def doubleBytes(d: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
    buf.putDouble(d).array()
  }

  // ---- 9) Stream graph ----
  val source: Source[(String,(Int,Double)), Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(config.getString("app.kafka.input-topic")))
      .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
      .mapAsync(1)(msg => Future {
        val Array(n, v) = msg.value.split("\\|", 2)
        (msg.key(), (n.toInt, v.toDouble))
      }(kafkaEc))

  val aggregator = Flow[(String,(Int,Double))]
    .buffer(1024, OverflowStrategy.backpressure)
    .groupedWithin(500, 200.millis)
    .mapAsyncUnordered(neuronCount) { batch =>
      Future {
        val completed = ListBuffer.empty[String]
        val ttlSec    = ttl.toSeconds.toInt

        // pipeline SETRANGE + INCRBY + EXPIRE
        val incrFuts = batch
          .groupBy(_._1)
          .map { case (id, elems) =>
            val blobKey  = s"1_$id".getBytes(StandardCharsets.UTF_8)
            val countKey = s"cnt:1:$id".getBytes(StandardCharsets.UTF_8)

            elems.foreach { case (_, (idx, dbl)) =>
              asyncCmds.setrange(blobKey, idx * 8L, doubleBytes(dbl))
            }
            val f = asyncCmds.incrby(countKey, elems.size)
            asyncCmds.expire(blobKey, ttlSec)
            asyncCmds.expire(countKey, ttlSec)
            (id, blobKey, countKey, f)
          }.toList

        asyncCmds.flushCommands()

        // collect results, compute prediction, delete counter
        incrFuts.foreach { case (id, blobKey, countKey, fut) =>
          if (fut.get() >= neuronCount) {
            val raw = syncCmds.get(blobKey)
            val buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
            val arr = Array.ofDim[Double](neuronCount)
            var i   = 0
            while (i < neuronCount) { arr(i) = buf.getDouble(); i += 1 }
            val pred = arr.zipWithIndex.maxBy(_._1)._2
            asyncCmds.del(countKey)
            completed += s"$id,$pred\n"
          }
        }

        asyncCmds.flushCommands()
        completed.toList
      }(redisEc)
    }

  val uploads: Source[MultipartUploadResult, NotUsed] =
    source.via(aggregator)
      .mapConcat(identity)
      .map(ByteString(_))
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

  // ---- 10) Materialize and shutdown ----
  val done: Future[Done] = uploads.runWith(Sink.ignore)
  done.onComplete {
    case Success(_) =>
      asyncConn.close(); syncConn.close(); redisClient.shutdown(); system.terminate()
    case Failure(ex) =>
      asyncConn.close(); syncConn.close(); redisClient.shutdown(); system.terminate()
  }
}
