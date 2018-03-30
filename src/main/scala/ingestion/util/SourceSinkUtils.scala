package ingestion.util

import ingestion.sinks.{FileSink, KafkaSink}
import ingestion.sources.KafkaSource
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceSinkUtils {

  //TODO: i dont like much this logic, think of something smoother

  def chooseSource(src: String, spark: SparkSession): DataFrame = {
    if (src.toLowerCase == "kafka") KafkaSource.getSource(spark)
    else throw new IllegalArgumentException(s"$src source is not supported.")
  }

  def chooseSink(df: DataFrame, sinkType: String): StreamingQuery = {
    if (sinkType.toLowerCase == "file") FileSink.getSink(df)
    else if (sinkType.toLowerCase() == "kafka") KafkaSink.getSink(df)
    else throw new IllegalArgumentException(s"$sinkType is not supported")
  }
}
