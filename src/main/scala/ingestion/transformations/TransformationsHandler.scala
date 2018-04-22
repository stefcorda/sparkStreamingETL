package ingestion.transformations

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._


object TransformationsHandler {

  private[this] val conf = ConfigFactory.load

  private val transFuncMap: Map[String, DataFrame => DataFrame] = Map[String, DataFrame => DataFrame](
    "add" -> ((df: DataFrame) => ColumnsHandler.addColumns(df)),
    "remove" -> ((df: DataFrame) => ColumnsHandler.removeColumns(df)),
    "date" -> ((df: DataFrame) => DateTrans.addCurrentTimestamp(df)),
    "watermark" -> ((df: DataFrame) => DateTrans.addWatermark(df)),
    "filter" -> ((df: DataFrame) => ColumnsHandler.filterOnColumns(df)),
    "select" -> ((df: DataFrame) => ColumnsHandler.selectOnColumns(df)),
    "count" -> ((df: DataFrame) => Aggregations.count(df))
  )

  val transformationsToApply: List[String] = conf.getStringList("transformations.order").asScala.toList


  def applyTransformations(df: DataFrame): DataFrame = transformationsToApply.foldLeft(df)(
    (df: DataFrame, funcKey: String) => transFuncMap.get(funcKey).fold(df)(f => f(df))
  )

  def applyTransformations(df: DataFrame, transformationsList: List[String]): DataFrame = transformationsList.foldLeft(df)(
    (df: DataFrame, funcKey: String) => transFuncMap.get(funcKey).fold(df)(f => f(df))
  )
}
