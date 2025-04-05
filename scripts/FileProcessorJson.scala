package app

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import software.amazon.awssdk.services.s3.S3Client

import java.io.FileNotFoundException
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Paths
import java.util.Properties
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.net.URI
import akka.stream.scaladsl.StreamConverters

object FileProcessorJsonHttp extends App {

  // Actor system and materializer for Akka Streams and HTTP
  implicit val system: ActorSystem = ActorSystem("akka-http-system")
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Configure Redis Jedis Pool
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(10)
  val jedisPool = new JedisPool(poolConfig, "localhost", 6379, 5000, null, false)

  // Configure Kafka Producer
  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

  val s3Client = S3Client.builder()
    .endpointOverride(new URI("http://localhost:9000"))
    .forcePathStyle(true)  // <- add this!
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("admin", "admin123")))
    .region(Region.US_EAST_1)
    .build()


  val getObjectRequest = GetObjectRequest.builder()
    .bucket("my-bucket")
    .key("mnist.csv")
    .build()

  val s3ObjectInputStream = s3Client.getObject(getObjectRequest)

  val delimiter = ","
  val parallelism = 4

  // Helper function to convert an array of doubles to a byte array
  def doubleArrayToBytes(data: Array[Double]): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(data.length * java.lang.Double.BYTES)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN) // Set to little-endian for NumPy compatibility
    data.foreach(byteBuffer.putDouble)
    byteBuffer.array()
  }

  // Define the file processing as a function returning a Future
  def processFile(): Future[Unit] = {
    val source = StreamConverters.fromInputStream(() => s3ObjectInputStream)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 65536, allowTruncation = true))
      .map(_.utf8String)
      .drop(1) // Skip header if needed
      .take(1000)
      .zipWithIndex

    val processFlow = Flow[(String, Long)].mapAsync(parallelism) {
      case (line, index) =>
        Future {
          val columns = line.split(delimiter).map(_.toDouble)
          val label = columns.last.toInt.toString
          val data = columns.slice(0, columns.length - 1)

          // Convert data to byte array
          val byteData = doubleArrayToBytes(data)

          // Store to Redis
          val redis = jedisPool.getResource
          try {
            redis.set(s"streams:$index:initial_data".getBytes, byteData)
            redis.hset("streams:images_label", index.toString, label)
          } finally {
            redis.close() // Always close the Redis connection
          }

//          val kafkaMessage = s"""layer_0|$index"""
//          kafkaProducer.send(new ProducerRecord[String, String]("activate-layer", null, "0", kafkaMessage))
          val kafkaMessage = s"""$index"""
          kafkaProducer.send(new ProducerRecord[String, String]("layer-0", null, null, kafkaMessage))

          // Introduce a sleep of 0.5 seconds between sends
          Thread.sleep(500)
        }
    }

    val result = source.via(processFlow).runWith(Sink.ignore)

    result.map(_ => ()) // Convert the Future to Future[Unit]
  }

  // Define the HTTP route
  val route =
    path("processJson") {
      get {
        // Launch the processing in a separate Future (fire and forget)
        Future { processFile() }
        complete("Processing started asynchronously.")
      }
    }


  // Start the HTTP server on localhost:8080
  val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)
  println("Server online at http://localhost:8081/\nTry: curl http://localhost:8081/processJson")

  // (Optional) Add shutdown hook if you want to gracefully terminate the server and resources
//  sys.addShutdownHook {
//    bindingFuture
//      .flatMap(_.unbind())
//      .onComplete { _ =>
//        jedisPool.close()
//        kafkaProducer.close()
//        system.terminate()
//      }
//  }
}