package ingestion.util

import ingestion.sinks.{ConsoleSink, FileSink, KafkaSink}
import ingestion.sources.{FileSource, KafkaSource}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceSinkUtils {

  //TODO: i dont like much this logic, think of something smoother

  def chooseSource(src: String, spark: SparkSession): DataFrame = {
    if (src.toLowerCase == "kafka") KafkaSource.getSource(spark)
    else if (src.toLowerCase == "file") FileSource.getSource(spark)
    else throw new IllegalArgumentException(s"$src source is not supported.")
  }

  def chooseSink(snk: String, df: DataFrame): StreamingQuery = {
    if (snk.toLowerCase == "file") FileSink.getSink(df)
    else if (snk.toLowerCase == "kafka") KafkaSink.getSink(df)
    else if (snk.toLowerCase == "console") ConsoleSink.getSink(df)
    else throw new IllegalArgumentException(s"$snk is not supported")
  }
}
