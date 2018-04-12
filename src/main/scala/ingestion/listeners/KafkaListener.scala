package ingestion.listeners


import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryListener

import scala.collection.JavaConversions._


class KafkaListener extends StreamingQueryListener {

  private[this] val conf = ConfigFactory.load
  private val topic = conf.getString("listeners.kafka.topic")

  val kafkaProperties: java.util.Map[String, Object] = mapAsJavaMap(
    Map(
      "bootstrap.servers" -> conf.getString("listeners.kafka.servers"),
      "key.serializer" -> conf.getString("listeners.kafka.keySerializer"),
      "value.serializer" -> conf.getString("listeners.kafka.valueSerializer")
    )
  )

  val producer = new KafkaProducer[String, String](kafkaProperties)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    producer.send(new ProducerRecord[String, String](topic, s"query started at time ${new Date()}"))
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    producer.send(new ProducerRecord(topic, event.progress.json))
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    producer.send(
      new ProducerRecord[String, String](
        topic,
        event.exception.fold(s"query for terminated with success")((ex: String) => s"query terminated with exception: $ex")
      )
    )
  }
}


