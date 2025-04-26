package app

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.Flow
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.ExecutionContext
import scala.collection.mutable

/**
 * Aggregates neuron outputs per imageId.
 * Emits (imageId, Array[Double]) only when neuronCount outputs have been collected.
 */
class NeuronAggregatorFinal(neuronCount: Int)
  extends GraphStage[FlowShape[(String, (Int, Double)), (String, Array[Double])]] {

  val in: Inlet[(String, (Int, Double))] = Inlet("NeuronAggregatorFinal.in")
  val out: Outlet[(String, Array[Double])] = Outlet("NeuronAggregatorFinal.out")
  override val shape: FlowShape[(String, (Int, Double)), (String, Array[Double])] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
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
          state -= imageId
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

  // Kafka consumer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")

  // Kafka producer settings
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG, "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG, "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG, "1")

  val neuronCount = 10

  // Flow: Convert aggregated outputs into Kafka ProducerRecord
  val predictionToProducerRecord: Flow[(String, Array[Double]), ProducerRecord[String, String], _] =
    Flow[(String, Array[Double])].map { case (imageId, outputs) =>
      val prediction = outputs.zipWithIndex.maxBy(_._1)._2 // index of max neuron value
      val messageValue = s"$imageId|$prediction"
      new ProducerRecord[String, String]("layer-output", imageId, messageValue)
    }

  // Stream definition
  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      val Array(neuronIdStr, valueStr) = msg.value().split("\\|", 2)
      (msg.key(), (neuronIdStr.toInt, valueStr.toDouble))
    }
    .via(new NeuronAggregatorFinal(neuronCount))
    .via(predictionToProducerRecord)
    .runWith(Producer.plainSink(producerSettings))
    .onComplete { result =>
      println(s"Stream completed: $result")
      system.terminate()
    }
}
