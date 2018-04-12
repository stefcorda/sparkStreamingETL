package ingestion.listeners

import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

object ListenersHandler {

  lazy val listenerMap: Map[String, StreamingQueryListener] = Map(
    "kafka" -> new KafkaListener(),
    "file" -> new FileListener()
  )

  private[this] val conf = ConfigFactory.load
  val listeners: List[String] = conf.getStringList("listeners.listenersToApply").asScala.toList

  def checkAndAddListeners(spark: SparkSession): Unit =
    listeners.foldLeft()(
      (_, listenerName) => {
        listenerMap
          .get(listenerName)
          .fold(println(s"failed to add listener $listenerName"))(spark.streams.addListener)
      })

}
