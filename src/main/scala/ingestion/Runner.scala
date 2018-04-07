package ingestion


import com.typesafe.config.ConfigFactory
import ingestion.transformations.{ColumnsHandler, DateTrans}
import ingestion.util.SourceSinkUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import util.Implicits.dateFormatISO8601
import org.apache.spark.sql.functions


object Runner {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ingestion")
    .master("local[*]")
    .getOrCreate()


  spark.conf.set("spark.sql.streaming.checkpointLocation", ConfigFactory.load.getString("checkpointLocation"))
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    require(args.length == 2, "USAGE: <source>, <sink>")

    val (sourceType, sinkType) = (args(0), args(1))

    val input: DataFrame = SourceSinkUtils.chooseSource(sourceType, spark)

    val output: DataFrame = DateTrans.addCurrentTimestamp(input) //just adding process time for now

    val sink: StreamingQuery = SourceSinkUtils.chooseSink(sinkType, output)

    sink
      .awaitTermination()

  }
}
