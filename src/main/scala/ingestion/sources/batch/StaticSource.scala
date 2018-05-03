package ingestion.sources.batch

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.JavaConverters._

abstract class StaticSource(val src: String) {

  val conf: Config = ConfigFactory.load

  private[batch] val sourcePrefix = s"sources.static.$src"

  private[batch] val optionParamPrefix = s"$sourcePrefix.optional."
  private[batch] val optionalParams: List[String] = conf.getObject(s"$sourcePrefix.optional").keySet().asScala.toList
  private[batch] val requiredParamPrefix = s"$sourcePrefix.required."
  private[batch] val requiredParams: List[String] = conf.getObject(s"$sourcePrefix.required").keySet().asScala.toList

  def getSource(spark: SparkSession): DataFrame

  def addOptionalParam(dfr: DataFrameReader, param: String): DataFrameReader = addParam(dfr, param, optionParamPrefix)

  def addRequiredParam(dfr: DataFrameReader, param: String): DataFrameReader = addParam(dfr, param, requiredParamPrefix)

  def applyOptParams(dfr: DataFrameReader): DataFrameReader = optionalParams.foldLeft(dfr)(addOptionalParam)

  def applyRequiredParams(dfr: DataFrameReader): DataFrameReader = requiredParams.foldLeft(dfr)(addRequiredParam)

  def applyAllParams(dfr: DataFrameReader): DataFrameReader = applyOptParams(applyRequiredParams(dfr))

  private def addParam(dfr: DataFrameReader, param: String, prefix: String): DataFrameReader = {
    val paramValue = conf.getString(s"$prefix$param")
    dfr.option(param, paramValue)
  }
}
