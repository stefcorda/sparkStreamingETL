package ingestion.util

import com.typesafe.config.{Config, ConfigFactory}
import ingestion.sinks.{ConsoleSink, ElasticSink, FileSink, KafkaSink}
import ingestion.sources.{FileSource, KafkaSource}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.DataFrame
import JoinableSourcesUtils.getJoinableSources

object SourceSinkUtils {

  private[util] val conf: Config = ConfigFactory.load

  private lazy val sourcesMap: Map[String, DataFrame] = Map[String, DataFrame](
    "kafka" -> new KafkaSource().source,
    "file" -> new FileSource().source,
    "joinables" -> getJoinableSources
  )

   private [util] lazy val joinableSourcesMap: Map[String, String => DataFrame] = Map(
    "kafka" -> ((sourceName: String) => KafkaSource.getSourceAsJoinable(sourceName)),
    "file" -> ((sourceName: String) => FileSource.getSourceAsJoinable(sourceName))
  )

  private lazy val sinksMap: Map[String, DataFrame => StreamingQuery] = Map[String, DataFrame => StreamingQuery](
    "kafka" -> ((df: DataFrame) => KafkaSink.getSink(df)),
    "file" -> ((df: DataFrame) => FileSink.getSink(df)),
    "console" -> ((df: DataFrame) => ConsoleSink.getSink(df)),
    "es" -> ((df: DataFrame) => ElasticSink.getSink(df))
  )

  def chooseSource(src: String): DataFrame = {
    require(sourcesMap.contains(src.toLowerCase), s"source $src is not supported")
    sourcesMap(src.toLowerCase)
  }

  def chooseSink(snk: String, df: DataFrame): StreamingQuery = {
    require(sinksMap.contains(snk.toLowerCase), s"$snk is not supported")
    sinksMap(snk.toLowerCase)(df)
  }
}
