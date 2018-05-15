package ingestion.transformations

import java.sql.Timestamp
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class DateTrans(prefixPath: String) {

  private[transformations] val conf = ConfigFactory.load

  def addCurrentTimestamp(df: DataFrame): DataFrame = {
    val timestampColumn = conf.getString(s"$prefixPath.date.column")
    df
      .withColumn(timestampColumn, lit(new Timestamp(new Date().getTime)))
  }

  def addWatermark(df: DataFrame): DataFrame = {
    val watermarkColumn = conf.getString(s"$prefixPath.watermark.column")
    val duration = conf.getString(s"$prefixPath.watermark.duration")
    df
      .withWatermark(watermarkColumn, duration)
  }
}
