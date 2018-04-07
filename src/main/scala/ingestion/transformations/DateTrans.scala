package ingestion.transformations

import java.sql.Timestamp
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object DateTrans {

  private[transformations] val  conf = ConfigFactory.load

  def addCurrentTimestamp(df: DataFrame): DataFrame = {
    val timestampColumn = conf.getString("transformations.date.column")
    df
      .withColumn(timestampColumn, lit(new Timestamp(new Date().getTime)))
  }

  def addWatermark(df: DataFrame): DataFrame = {
    val watermarkColumn = conf.getString("transformations.watermark.column")
    val duration = conf.getString("transformations.watermark.duration")
    df
      .withWatermark(watermarkColumn, duration)
  }
}
