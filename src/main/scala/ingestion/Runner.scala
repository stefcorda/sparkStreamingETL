package ingestion


import com.typesafe.config.{Config, ConfigFactory}
import ingestion.listeners.ListenersHandler
import ingestion.transformations.TransformationsHandler
import ingestion.util.SourceSinkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery


object Runner {
  val conf: Config = ConfigFactory.load

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ingestion")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.streaming.checkpointLocation", conf.getString("application.checkpointLocation"))
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    ListenersHandler.checkAndAddListeners(spark)

    val (sourceType, sinkType) = (conf.getString("sources.sourceToApply"), conf.getString("sinks.sinkToApply"))

    val input: DataFrame = SourceSinkUtils.chooseSource(sourceType)

    val output: DataFrame = TransformationsHandler().applyTransformations(input)

    val sink: StreamingQuery = SourceSinkUtils.chooseSink(sinkType, output)

    sink
      .awaitTermination()

  }
}
