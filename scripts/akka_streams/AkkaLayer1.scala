package app

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl._
import akka.stream._
import akka.stream.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.mutable
import scala.concurrent._


object AkkaLayer1 extends App {
  implicit val system: ActorSystem   = ActorSystem("streams-1-opt")
  implicit val mat:    Materializer  = ActorMaterializer()
  implicit val ec:     ExecutionContext = system.dispatcher

  val neuronCount     = 10
  val maxKeysInFlight = 1024

  // Semaphore to limit parallel keys
  private val sem = new java.util.concurrent.Semaphore(maxKeysInFlight)
  private val blockingDispatcher = system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")

  private def withPermit[T](f: => Future[T]): Future[T] =
    Future(sem.acquire())(blockingDispatcher)
      .flatMap(_ => f.transform { res => sem.release(); res })(ec)

  // Aggregation+offset batching stage
  val aggregateStage: Flow[(String, (Int, Double), ConsumerMessage.CommittableOffset),
    (String, Array[Double], ConsumerMessage.CommittableOffsetBatch), NotUsed] =
    Flow[(String, (Int, Double), ConsumerMessage.CommittableOffset)]
      .statefulMapConcat { () =>
        val buf    = mutable.Map.empty[String, Array[Double]]
        val counts = mutable.Map.empty[String, Int]
        val batches= mutable.Map.empty[String, ConsumerMessage.CommittableOffsetBatch]

        elem => {
          val (id, (nid, v), offset) = elem
          val arr = buf.getOrElseUpdate(id, Array.ofDim[Double](neuronCount))
          arr(nid) = v
          val cnt = counts.getOrElse(id, 0) + 1
          counts.update(id, cnt)

          val batch = batches.get(id) match {
            case Some(b) => b.updated(offset)
            case None    => ConsumerMessage.CommittableOffsetBatch.empty.updated(offset)
          }
          batches.update(id, batch)

          if (cnt == neuronCount) {
            buf.remove(id); counts.remove(id); batches.remove(id)
            List((id, arr, batch))
          } else Nil
        }
      }

  // Kafka consumer settings
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("streams-1-opt-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000")
    .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576")
    .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50")

  // Kafka producer settings
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
    .withProperty(ProducerConfig.LINGER_MS_CONFIG,        "5")
    .withProperty(ProducerConfig.BATCH_SIZE_CONFIG,       "256000")
    .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    .withProperty(ProducerConfig.ACKS_CONFIG,             "1")

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics("layer-1-streams"))
    .map { msg =>
      val Array(nStr, vStr) = msg.record.value.split("\\|", 2)
      (msg.record.key, (nStr.toInt, vStr.toDouble), msg.committableOffset)
    }
    .via(aggregateStage)
    .mapAsyncUnordered(maxKeysInFlight) { case (id, arr, batch) =>
      withPermit {
        // Compute max neuron index
        Future.successful((id, arr.zipWithIndex.maxBy(_._1)._2, batch))
      }
    }
    .map { case (id, maxIdx, batch) =>
      val msg = new ProducerRecord[String, String]("layer-output", id, s"$id|$maxIdx")
      ProducerMessage.single(msg, batch)
    }
    .via(Producer.flexiFlow(producerSettings))
    .mapAsync(parallelism = 4)(_.passThrough.commitScaladsl())
    .runWith(Sink.ignore)
    .onComplete { _ =>
      system.terminate()
    }
}
