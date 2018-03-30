package ingestion.sinks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{DataStreamWriter, ProcessingTime, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

object FileSink extends Sink("file") {
  private[this] val conf: Config = ConfigFactory.load

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
