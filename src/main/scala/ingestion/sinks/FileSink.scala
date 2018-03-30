package ingestion.sinks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery, Trigger}

object FileSink extends Sink("file") {
  private[this] val conf: Config = ConfigFactory.load

  val path: String = conf.getString(s"sinks.$snk.required.path")
  val format: String = conf.getString(s"sinks.$snk.required.format")
  val outputMode: String = conf.getString(s"sinks.$snk.optional.outputMode")
  val refreshTime: Long = conf.getLong(s"sinks.$snk.optional.refreshTime")

  def getSink(df: DataFrame): StreamingQuery = {
    val baseSink = df
      .writeStream
      .format(format)
      .outputMode(outputMode)
      .trigger(Trigger.ProcessingTime(refreshTime)) //check every X ms for new files

    val optBaseSink = applyOptParams(baseSink)
    optBaseSink.start(path)

  }


}
