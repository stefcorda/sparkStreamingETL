package ingestion

import com.typesafe.config.ConfigFactory
import ingestion.util.{DateFormatter, SourceSinkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.StreamingQuery
import util.Implicits.dateFormatISO8601


object Runner {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ingestion")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.streaming.checkpointLocation", ConfigFactory.load.getString("checkpointLocation"))
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    if (args.length != 2)
      throw new IllegalArgumentException("USAGE: <source>, <sink>")

    val (sourceType, sinkType) = (args(0), args(1))

    val input: DataFrame = SourceSinkUtils.chooseSource(sourceType, spark)

    val output: DataFrame = input //THINK OF SOME ETL

    val sink: StreamingQuery = SourceSinkUtils.chooseSink(output, sinkType)

    sink
      .awaitTermination()

  }
}
