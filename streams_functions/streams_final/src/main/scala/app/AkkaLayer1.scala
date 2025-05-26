package app

import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}
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

object AkkaLayer1 extends App {
  // ---- 1) Environment variables helper ----
  def env(name: String): String =
    sys.env.getOrElse(name, throw new IllegalArgumentException(s"Env var '$name' not set"))

  // ---- 2) Load static settings from application.conf ----
  val config      = ConfigFactory.load()
  val neuronCount = config.getInt("akka.stream.neuron-count")
  val ttl         = config.getDuration("akka.stream.redis-ttl-sec").toMillis.millis
  val bucket      = config.getString("app.s3.bucket")

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

  // Dedicated dispatchers for blocking or I/O operations
  private val kafkaEc: ExecutionContext = system.dispatchers.lookup("akka.actor.kafka-consumer-dispatcher")
  private val s3Ec:    ExecutionContext = system.dispatchers.lookup("akka.actor.s3-dispatcher")
  private val redisEc: ExecutionContext = system.dispatchers.lookup("akka.actor.redis-dispatcher")

  // ---- 5) Redis client setup (Lettuce) ----
  private val redisClient: RedisClient = RedisClient.create(redisUri)
  // Asynchronous connection (pipelining enabled with manual flush)
  private val asyncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    redisClient.connect(ByteArrayCodec.INSTANCE)
  private val asyncCmds: RedisAsyncCommands[Array[Byte], Array[Byte]] =
    asyncConn.async()
  asyncCmds.setAutoFlushCommands(false)  // disable auto-flush for batching
  // Synchronous connection (for immediate GET operations on Redis)
  private val syncConn: StatefulRedisConnection[Array[Byte], Array[Byte]] =
    redisClient.connect(ByteArrayCodec.INSTANCE)
  private val syncCmds = syncConn.sync()

  // ---- 6) Kafka consumer settings ----
  val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaBootstrap)
      .withGroupId("akka-streams1-group")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
      .withStopTimeout(0.seconds)  // fast shutdown of consumer without delay

  // ---- 7) S3 client & Alpakka S3 settings ----
  // Initialize the AWS S3 async client (Netty NIO HTTP client for non-blocking I/O)
  private val awsCreds = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  S3AsyncClient.builder()
    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
    .endpointOverride(URI.create(s3Endpoint))
    .region(SdkRegion.of(s3Region))
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .build()
  implicit val s3Settings: S3Settings = S3Settings(system)
    .withCredentialsProvider(StaticCredentialsProvider.create(awsCreds))
    .withS3RegionProvider(() => SdkRegion.of(s3Region))
    .withEndpointUrl(s3Endpoint)
    .withAccessStyle(AccessStyle.PathAccessStyle)    // use path-style access (minio compatibility)
    .withBufferType(MemoryBufferType)                // memory buffer for uploads

  // ---- 8) Helpers ----
  private val tsFmt = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS'Z'").withZone(ZoneOffset.UTC)
  private def makeKey(): String = {
    // Generate a unique S3 object key with timestamp and random suffix
    val timestamp = tsFmt.format(Instant.now())
    val suffix    = Random.alphanumeric.take(4).mkString
    s"predictions/part-$timestamp-$suffix.csv"
  }
  private def doubleBytes(d: Double): Array[Byte] = {
    // Serialize a Double into little-endian byte array (8 bytes)
    ByteBuffer.allocate(java.lang.Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(d).array()
  }

  // ---- 9) Stream processing graph ----
  // Define the Kafka source: consumes messages from Kafka and parses them
  val kafkaSource: Source[(String, (Int, Double)), Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(config.getString("app.kafka.input-topic")))
      .withAttributes(ActorAttributes.dispatcher("akka.actor.kafka-consumer-dispatcher"))
      // Parse incoming Kafka messages: key remains the ID, value is "index|doubleValue"
      .mapAsync(1) { msg =>
        Future {
          val Array(idxStr, valStr) = msg.value.split("\\|", 2)
          (msg.key(), (idxStr.toInt, valStr.toDouble))
        }(kafkaEc)
      }

  // Flow to aggregate predictions per ID using Redis
  val aggregatorFlow: Flow[(String, (Int, Double)), List[String], NotUsed] =
    Flow[(String, (Int, Double))]
      .buffer(1024, OverflowStrategy.backpressure)              // buffer for bursty input
      .groupedWithin(500, 200.millis)                           // batch up to 500 messages or 200ms of data
      .mapAsyncUnordered(parallelism = neuronCount) { batch =>  // parallel processing of batches
        // Run Redis pipeline operations on a dedicated dispatcher thread pool
        Future(processBatch(batch))(redisEc)
      }

  // Helper function to process a batch of messages for different IDs
  private def processBatch(batch: Seq[(String, (Int, Double))]): List[String] = {
    val completedPredictions = ListBuffer.empty[String]
    val ttlSeconds = ttl.toSeconds.toInt

    // Group incoming values by their ID for aggregation
    val groupedById = batch.groupBy(_._1)
    // Queue Redis commands for each ID in the batch
    val results = groupedById.toList.map { case (id, elems) =>
      val blobKey  = s"1_$id".getBytes(StandardCharsets.UTF_8)    // Redis key for stored values
      val countKey = s"cnt:1:$id".getBytes(StandardCharsets.UTF_8) // Redis key for count of values

      // Queue SETRANGE for each value (store the double at the given index in the blob)
      elems.foreach { case (_, (idx, value)) =>
        asyncCmds.setrange(blobKey, idx.toLong * 8L, doubleBytes(value))
      }
      // Queue INCRBY to count how many values have been stored for this ID
      val countFuture = asyncCmds.incrby(countKey, elems.size)
      // Set expiration on the keys to auto-cleanup after TTL (for incomplete aggregates)
      asyncCmds.expire(blobKey, ttlSeconds)
      asyncCmds.expire(countKey, ttlSeconds)

      (id, blobKey, countKey, countFuture)
    }
    // Flush all batched commands to Redis in one go
    asyncCmds.flushCommands()

    // For each ID, check if we've collected all required values
    results.foreach { case (id, blobKey, countKey, countFuture) =>
      // Get the updated count (blocking call on Redis future, running on redisEc thread)
      if (countFuture.get() >= neuronCount) {
        // All values received for this ID: retrieve blob and compute prediction
        val blobData = syncCmds.get(blobKey)
        if (blobData != null) {
          // Deserialize all double values from the blob
          val buf    = ByteBuffer.wrap(blobData).order(ByteOrder.LITTLE_ENDIAN)
          val values = Array.ofDim[Double](neuronCount)
          var i = 0
          while (i < neuronCount) { values(i) = buf.getDouble(); i += 1 }
          // Determine the index of the maximum value (predicted class)
          val predictedIndex = values.zipWithIndex.maxBy(_._1)._2
          completedPredictions += s"$id,$predictedIndex\n"
        }
        // Queue deletion of Redis keys for the completed ID (cleanup)
        asyncCmds.del(countKey)
        asyncCmds.del(blobKey)
      }
    }
    // Flush deletion commands if any keys were completed
    if (completedPredictions.nonEmpty) {
      asyncCmds.flushCommands()
    }
    completedPredictions.toList
  }

  // ---- 10) Materialize stream and handle shutdown ----
  // Build the end-to-end stream: from Kafka to Redis aggregation to S3 upload
  val streamGraph: RunnableGraph[(Consumer.Control, Future[Done])] =
    kafkaSource
      .via(aggregatorFlow)
      .async  // async boundary to isolate upstream (Redis) from downstream (S3)
      .mapConcat(identity)  // flatten lists of completed predictions
      .map(ByteString(_))   // convert each prediction line to ByteString
      .groupedWithin(500, 3.minutes)  // batch up to 500 prediction lines or 3 minutes of data
      .mapAsyncUnordered(parallelism = 4) { chunk =>
        // For each chunk of predictions, initiate an S3 multipart upload (async)
        Future {
          val key    = makeKey()  // unique S3 object key for this chunk
          val header = ByteString("image_id,prediction\n")
          // Prepare source for upload: CSV header followed by all prediction lines in the chunk
          val dataSource: Source[ByteString, NotUsed] =
            Source.single(header) ++ Source(chunk.toList)
          dataSource.runWith(
            S3.multipartUpload(bucket, key)
              .withAttributes(S3Attributes.settings(s3Settings))
          )
        }(s3Ec).flatMap(identity)(s3Ec)  // flatten Future[Future[MultipartUploadResult]] to Future
      }
      .toMat(Sink.ignore)(Keep.both)  // materialize to get both Consumer.Control and stream completion

  // Run the stream
  val (control, done) = streamGraph.run()

  // Atomic flag to ensure shutdown sequence is executed only once
  private val shutdownInitiated = new AtomicBoolean(false)
  def shutdownResources(): Unit = {
    if (shutdownInitiated.compareAndSet(false, true)) {
      // Flush any pending Redis commands
      try asyncCmds.flushCommands() catch {
        case e: Throwable => system.log.warning("Failed to flush Redis commands on shutdown: {}", e.getMessage)
      }
      // Close Redis connections and client
      try {
        asyncConn.close()
        syncConn.close()
        redisClient.shutdown()
      } catch {
        case e: Throwable => system.log.warning("Error closing Redis client: {}", e.getMessage)
      }
      // Terminate the ActorSystem (will also close the Kafka consumer via CoordinatedShutdown)
      system.terminate()
    }
  }

  // Callback to shut down resources when the stream completes or fails
  done.onComplete {
    case Success(_) =>
      system.log.info("Stream completed, shutting down resources.")
      shutdownResources()
    case Failure(err) =>
      system.log.error(err, "Stream failed, shutting down resources.")
      shutdownResources()
  }(system.dispatcher)

  // Ensure graceful shutdown on JVM termination (e.g., SIGTERM in Kubernetes)
  sys.addShutdownHook {
    system.log.info("Shutdown hook triggered, stopping Kafka consumer and closing resources...")
    try control.stop() catch {
      case e: Throwable => system.log.warning("Error stopping Kafka consumer: {}", e.getMessage)
    }
    // Invoke resource shutdown (idempotent)
    shutdownResources()
  }
}
