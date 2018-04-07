package ingestion.transformations

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._


object TransformationsHandler {

  private[this] val conf = ConfigFactory.load

  private val transFuncMap: Map[String, DataFrame => DataFrame] = Map[String, DataFrame => DataFrame](
    "add" -> ((df: DataFrame) => ColumnsHandler.addColumns(df)),
    "remove" -> ((df: DataFrame) => ColumnsHandler.removeColumns(df)),
    "date" -> ((df:DataFrame) => DateTrans.addCurrentTimestamp(df)),
    "watermark" -> ((df: DataFrame) => DateTrans.addWatermark(df))
  )

  //TODO: order is not being respected. Think about it.
  val transformationsToApply: List[String]= conf.getObject("transformations").keySet().asScala.toList


  def applyTransformations(df: DataFrame): DataFrame = transformationsToApply.foldLeft(df)(
    (df: DataFrame, funcKey: String) => transFuncMap.get(funcKey) match {
      case Some(fun) => fun(df)
      case None =>
        println(s"$funcKey not supported")
        df
    }
  )
}
