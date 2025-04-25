package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable

// AWS SDK for S3 / MinIO
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import java.net.URI

// --- Neuron Aggregator ---
class NeuronAggregatorFinal(neuronCount: Int)
  extends GraphStage[FlowShape[(String, (Int, Double)), (String, Array[Double])]] {

  val in  = Inlet[(String, (Int, Double))]("NeuronAggregatorFinal.in")
  val out = Outlet[(String, Array[Double])]("NeuronAggregatorFinal.out")
  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    private val state = mutable.Map.empty[String, (Array[Double], mutable.BitSet, Int)]

    setHandler(in, new akka.stream.stage.InHandler {
      override def onPush(): Unit = {
        val (imageId, (neuronId, value)) = grab(in)
        val (arr, seen, count) = state.getOrElseUpdate(
          imageId,
          (Array.fill[Double](neuronCount)(0.0), mutable.BitSet.empty, 0)
        )
        if (!seen.contains(neuronId)) {
          arr(neuronId) = value
          seen += neuronId
          state.update(imageId, (arr, seen, count + 1))
        }

        if (state(imageId)._3 >= neuronCount) {
          state -= imageId // ✅ Remove from state (correct)
          push(out, (imageId, arr))
        } else {
          pull(in)
        }
      }
    })

    setHandler(out, new akka.stream.stage.OutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}

object AkkaLayer1 extends App {
  implicit val system: ActorSystem = ActorSystem("streams-1")
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContext = system.dispatcher

  // --- Kafka Consumer Settings ---
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")

  val neuronCount = 10

  // --- S3 Client (MinIO) ---
  val s3Client = S3Client.builder()
    .endpointOverride(new URI("http://localhost:9000")) // Your MinIO endpoint
    .forcePathStyle(true)                              // Important for MinIO
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("admin", "admin123")))
    .region(Region.US_EAST_1)
    .build()

  val bucketName = "predictions" // Make sure this exists!

  // --- File Writing Logic ---
  @volatile var fileCounter = 0

  def writeBatchToS3(batch: Seq[(String, Array[Double])]): Future[Unit] = Future {
    val fileName = f"predictions_batch_${fileCounter}%05d.csv"
    fileCounter += 1

    val header = "image_id,prediction\n"
    val rows = batch.map { case (imageId, arr) =>
      val maxIndex = arr.zipWithIndex.maxBy(_._1)._2
      s"$imageId,$maxIndex"
    }.mkString("\n")
    val content = header + rows + "\n"

    // Write to temp local file
    val tempFilePath = Paths.get(s"/tmp/$fileName")
    Files.write(tempFilePath, content.getBytes(StandardCharsets.UTF_8))

    // Correct S3 upload with RequestBody
    val putRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(fileName)
      .build()

    s3Client.putObject(putRequest, RequestBody.fromFile(tempFilePath.toFile))
    println(s"✅ Uploaded $fileName to S3 bucket '$bucketName'")

    // Optional: Clean up temp file after upload
    Files.deleteIfExists(tempFilePath)
  }

  // --- Stream Pipeline ---
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      val Array(nStr, vStr) = msg.value().split("\\|", 2)
      (msg.key(), (nStr.toInt, vStr.toDouble))
    }
    .via(new NeuronAggregatorFinal(neuronCount))
    .groupedWithin(500, 5.seconds)
    .mapAsync(4)(writeBatchToS3)
    .runWith(Sink.ignore)
    .onComplete { result =>
      println(s"Stream completed: $result")
      s3Client.close()
      system.terminate()
    }
}
