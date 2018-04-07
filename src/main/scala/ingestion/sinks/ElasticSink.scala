package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object ElasticSink extends Sink("es") {

  val index: String = conf.getString("sinks.es.index")
  val logType: String = conf.getString("sinks.es.type")

  override def getSink(df: DataFrame): StreamingQuery = {
    val baseESSink = df
      .writeStream
      .format("org.elasticsearch.spark.sql")

    val configuredESSink = applyAllParams(baseESSink)

    configuredESSink
      .start(s"$index/$logType")
  }

}
