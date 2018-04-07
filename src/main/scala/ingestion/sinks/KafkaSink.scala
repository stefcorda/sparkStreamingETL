package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object KafkaSink extends Sink("kafka") {
  override def getSink(df: DataFrame): StreamingQuery = {

    val jsonDF = df.toJSON

    val baseKafkaSink = jsonDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") //TODO: handle dots in configuration...

    val kafkaSink = applyAllParams(baseKafkaSink)

    kafkaSink.start()
  }
}
