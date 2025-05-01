package app

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.processor.{ProcessorContext, TaskId}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import java.util.{Properties, UUID}
import scala.concurrent.{ExecutionContext, Future}

object StreamsLayer1 extends App {

  val NEURON_COUNT = 10
  val MAX_RECORDS = 500
  val MAX_DURATION: Duration = Duration.ofMinutes(3)

  val stateDir = "./kafka-streams-state"
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-1")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000")
  props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
  props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")
  props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
  props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "10")

  deleteDirectoryRecursively(new File(stateDir))

  val storeName = "state-store"
  val builder = new StreamsBuilder()

  builder.addStateStore(
    Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(storeName),
      Serdes.String,
      Serdes.String
    )
  )

  implicit val ec: ExecutionContext = ExecutionContext.global

  val inputStream: KStream[String, String] = builder.stream[String, String]("layer-1-streams")

  val mappedStream = inputStream.mapValues { value =>
    val Array(index, v) = value.split("\\|")
    (index.toInt, v.toDouble)
  }

  implicit val grouped: Grouped[String, (Int, Double)] = Grouped.`with`(Serdes.String, tupleSerde)

  mappedStream.transformValues(() => new ValueTransformerWithKey[String, (Int, Double), String] {
    var store: KeyValueStore[String, String] = _
    var taskPrefix: String = _

    override def init(context: ProcessorContext): Unit = {
      store = context.getStateStore(storeName).asInstanceOf[KeyValueStore[String, String]]
      val taskId: TaskId = context.taskId()
      taskPrefix = s"task_${taskId.topicGroupId}_${taskId.partition}"
    }

    override def transform(key: String, value: (Int, Double)): String = synchronized {
      val (index, score) = value
      val stateKey = s"state_$key"
      val arr = Option(store.get(stateKey)) match {
        case Some(serialized) => serialized.split(",").map(_.toDouble)
        case None => Array.fill(NEURON_COUNT)(0.0)
      }

      arr(index) = score
      store.put(stateKey, arr.mkString(","))

      if (arr.count(_ != 0.0) == NEURON_COUNT) {
        val prediction = arr.zipWithIndex.maxBy(_._1)._2
        val resultLine = s"$key,$prediction"
        store.delete(stateKey)

        println(s"[PREDICTED] $resultLine")

        val batchIndexKey = s"${taskPrefix}_batchIndex"
        val batchIndex = Option(store.get(batchIndexKey)).map(_.toInt).getOrElse(0)
        val batchKey = s"${taskPrefix}_batch_$batchIndex"
        val tsKey = s"${taskPrefix}_ts_$batchIndex"
        val countKey = s"${taskPrefix}_counter_$batchIndex"

        val now = Instant.now().toEpochMilli
        val currentBatch = Option(store.get(batchKey)).getOrElse("")
        val updated = if (currentBatch.isEmpty) resultLine else currentBatch + "\n" + resultLine
        store.put(batchKey, updated)

        val currentCount = Option(store.get(countKey)).map(_.toInt).getOrElse(0) + 1
        store.put(countKey, currentCount.toString)

        if (store.get(tsKey) == null) store.put(tsKey, now.toString)
        val batchStart = Option(store.get(tsKey)).map(_.toLong).getOrElse(now)

        if (currentCount >= MAX_RECORDS || (now - batchStart) >= MAX_DURATION.toMillis) {
          val completedBatch = store.get(batchKey)
          store.delete(batchKey)
          store.delete(tsKey)
          store.delete(countKey)
          Future(uploadToS3(completedBatch))

          store.put(batchIndexKey, (batchIndex + 1).toString)
        }
      }
      null
    }

    override def close(): Unit = {}
  }, storeName)

  val streams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()
  sys.ShutdownHookThread(streams.close())

  def uploadToS3(csvContent: String): Unit = {
    val bucket = "faasnn-bucket"
    val uuid = UUID.randomUUID().toString
    val keyPrefix = s"predictions/part-$uuid.csv"

    println(s"[S3 UPLOAD] Uploading file to $keyPrefix with ${csvContent.linesIterator.size} lines")

    val creds = AwsBasicCredentials.create("admin", "admin123")
    val s3Client = S3AsyncClient.builder()
      .credentialsProvider(StaticCredentialsProvider.create(creds))
      .endpointOverride(URI.create("http://localhost:9000"))
      .region(Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    val header = "image_i,prediction\n"
    val fullCsv = header + csvContent

    val request = PutObjectRequest.builder()
      .bucket(bucket)
      .key(keyPrefix)
      .build()

    s3Client.putObject(request, AsyncRequestBody.fromString(fullCsv, StandardCharsets.UTF_8))
      .whenComplete { (_, ex) =>
        if (ex != null) {
          println(s"[S3 ERROR] Failed to upload: ${ex.getMessage}")
          ex.printStackTrace()
        } else {
          println(s"[S3 SUCCESS] Uploaded to $keyPrefix")
        }
        s3Client.close()
      }
  }

  def deleteDirectoryRecursively(dir: File): Unit = {
    if (dir.exists()) {
      val files = dir.listFiles()
      if (files != null) files.foreach(deleteDirectoryRecursively)
      dir.delete()
    }
  }

  implicit def tupleSerde: Serde[(Int, Double)] = Serdes.serdeFrom(
    (_: String, tuple: (Int, Double)) => s"${tuple._1},${tuple._2}".getBytes,
    (_: String, bytes: Array[Byte]) => {
      val str = new String(bytes).split(",")
      (str(0).toInt, str(1).toDouble)
    }
  )
}
