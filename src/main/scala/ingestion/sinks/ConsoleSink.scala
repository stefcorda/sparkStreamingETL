package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import scala.util.Try

object ConsoleSink extends Sink("console") {

  override def getSink(df: DataFrame): StreamingQuery = {

    val outputMode: String = Try(conf.getString(s"sinks.$snk.optional.outputMode")).getOrElse("append")

    df
      .writeStream
      .format(s"$snk")
      .outputMode(outputMode)
      .start()

  }

}
