package ingestion.sources

import com.typesafe.config.{Config, ConfigFactory}
import ingestion.sinks.FileSink.conf
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileSource extends Source("file") {

  private[this] val conf: Config = ConfigFactory.load

  val path: String = conf.getString(s"sources.$src.required.path")
  val format: String = conf.getString(s"sources.$src.required.format")
  val outputMode: String = conf.getString(s"sources.$src.optional.outputMode")
  val refreshTime: Long = conf.getLong(s"sources.$src.optional.refreshTime")

  val schema: Either[Throwable, List[AnyRef]] = conf.getAnyRefList(s"sources.$src.schema") match {
    case ex: Throwable => Left(ex)
    case lst: List[AnyRef] => Right(lst)
  }

  override def getSource(spark: SparkSession): DataFrame = {
    val baseSource: DataStreamReader = spark
      .readStream
      .format(format)

    val fileSource = applyAllParams(baseSource)
    fileSource.load(path)

  }

  private def applySchema(df: DataFrame): DataFrame = {
    val schema = ???
    ???
  }

}
