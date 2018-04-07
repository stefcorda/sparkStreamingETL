package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object ConsoleSink extends Sink("console") {

  override def getSink(df: DataFrame): StreamingQuery = {
    df
      .writeStream
      .format(s"$snk")
      .start()
  }

}
