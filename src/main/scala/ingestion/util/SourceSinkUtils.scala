package ingestion.util

import ingestion.sinks.{ConsoleSink, ElasticSink, FileSink, KafkaSink}
import ingestion.sources.batch.ElasticSource
import ingestion.sources.{FileSource, KafkaSource}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceSinkUtils {

  private val sourcesMap: Map[String, SparkSession => DataFrame] = Map[String, SparkSession => DataFrame](
    "kafka" -> ((spark: SparkSession) => KafkaSource.getSource(spark)),
    "file" -> ((spark: SparkSession) => FileSource.getSource(spark)),
    "es" -> ((spark: SparkSession) => ElasticSource.getSource(spark))
  )

  private val sinksMap: Map[String, DataFrame => StreamingQuery] = Map[String, DataFrame => StreamingQuery](
    "kafka" -> ((df: DataFrame) => KafkaSink.getSink(df)),
    "file" -> ((df: DataFrame) => FileSink.getSink(df)),
    "console" -> ((df: DataFrame) => ConsoleSink.getSink(df)),
    "es" -> ((df: DataFrame) => ElasticSink.getSink(df))
  )

  def chooseSource(src: String, spark: SparkSession): DataFrame = {
    require(sourcesMap.contains(src.toLowerCase), s"source $src is not supported")
    sourcesMap(src.toLowerCase)(spark)
  }

  def chooseSink(snk: String, df: DataFrame): StreamingQuery = {
    require(sinksMap.contains(snk.toLowerCase), s"$snk is not supported")
    sinksMap(snk.toLowerCase)(df)
  }
}
