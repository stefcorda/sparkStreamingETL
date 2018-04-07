package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object KafkaSink extends Sink("kafka") {
  override def getSink(df: DataFrame): StreamingQuery = {

    val jsonDF = df.toJSON
    val kafkaServers = conf.getString(s"sinks.$snk.kafka-bootstrap-servers")

    val baseKafkaSink = jsonDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers) //TODO: handle dots in configuration...

    val kafkaSink = applyAllParams(baseKafkaSink)

    kafkaSink.start()
  }
}
