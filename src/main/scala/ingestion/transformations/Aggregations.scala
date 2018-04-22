package ingestion.transformations

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, window}

import scala.collection.JavaConverters._

object Aggregations {

  private[this] val conf = ConfigFactory.load

  def count(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val watermarkColumn: String = conf.getString("transformations.count.watermarkColumn")
    val (windowTime, slidingWindowTime) = (conf.getString("transformations.count.windowTime"), conf.getString("transformations.count.slidingTime"))
    val groupColumns: List[Column] = List(window(col(watermarkColumn), windowTime, slidingWindowTime)) ++
      conf.getStringList("transformations.count.columns").asScala.map(col)

    df.groupBy(
      groupColumns: _*
    )
      .count
  }

}
