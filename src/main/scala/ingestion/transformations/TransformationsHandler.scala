package ingestion.transformations

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._


class TransformationsHandler(prefixPath: String) {

  import TransformationsHandler._

  private[this] lazy val columnsHandler: ColumnsHandler = new ColumnsHandler(prefixPath)
  private[this] lazy val dateTrans: DateTrans = new DateTrans(prefixPath)
  private[this] lazy val aggregations: Aggregations = new Aggregations(prefixPath)


  private val transFuncMap: Map[String, DataFrame => DataFrame] = Map[String, DataFrame => DataFrame](
    "add" -> ((df: DataFrame) => columnsHandler.addColumns(df)),
    "remove" -> ((df: DataFrame) => columnsHandler.removeColumns(df)),
    "date" -> ((df: DataFrame) => dateTrans.addCurrentTimestamp(df)),
    "watermark" -> ((df: DataFrame) => dateTrans.addWatermark(df)),
    "filter" -> ((df: DataFrame) => columnsHandler.filterOnColumns(df)),
    "select" -> ((df: DataFrame) => columnsHandler.selectOnColumns(df)),
    "count" -> ((df: DataFrame) => aggregations.count(df))
  )

  val transformationsToApply: List[String] = conf.getStringList(s"$prefixPath.order").asScala.toList


  def applyTransformations(df: DataFrame): DataFrame = transformationsToApply.foldLeft(df)(
    (df: DataFrame, funcKey: String) => transFuncMap.get(funcKey).fold(df)(f => f(df))
  )

  def applyTransformations(df: DataFrame, transformationsList: List[String]): DataFrame = transformationsList.foldLeft(df)(
    (df: DataFrame, funcKey: String) => transFuncMap.get(funcKey).fold(df)(f => f(df))
  )
}

object TransformationsHandler {
  private[transformations] val conf = ConfigFactory.load

  def apply(): TransformationsHandler = new TransformationsHandler("transformations")


}