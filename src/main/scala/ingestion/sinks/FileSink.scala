package ingestion.sinks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object FileSink extends Sink("file") {

  val path: String = conf.getString(s"sinks.$snk.required.path")
  val format: String = conf.getString(s"sinks.$snk.required.format")
  val outputMode: String = conf.getString(s"sinks.$snk.optional.outputMode")

  def getSink(df: DataFrame): StreamingQuery = {
    val baseSink = df
      .writeStream
      .format(format)
      .outputMode(outputMode)

    val optSink = applyOptParams(baseSink)

    optSink.start(path)

  }

}
