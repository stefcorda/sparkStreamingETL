package ingestion.transformations

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, window}

import scala.collection.JavaConverters._

class Aggregations(prefixPath: String) {

  private[this] val conf = ConfigFactory.load

  def count(df: DataFrame): DataFrame = {
    val watermarkColumn: String = conf.getString(s"$prefixPath.count.watermarkColumn")
    val (windowTime, slidingWindowTime) = (conf.getString(s"$prefixPath.count.windowTime"), conf.getString(s"$prefixPath.count.slidingTime"))
    val groupColumns: List[Column] = List(window(col(watermarkColumn), windowTime, slidingWindowTime)) ++
      conf.getStringList(s"$prefixPath.count.columns").asScala.map(col)

    df.groupBy(groupColumns: _*).count
  }

}
