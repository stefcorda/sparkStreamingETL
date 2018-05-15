package ingestion.sources

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import Source._

import collection.JavaConverters._

abstract class Source(val src: String) {

  private[sources] lazy val optionParamPrefix = s"sources.$src.optional."
  private[sources] lazy val optionalParams: List[String] = conf.getObject(s"sources.$src.optional").keySet().asScala.toList
  private[sources] lazy val requiredParamPrefix = s"sources.$src.required."
  private[sources] lazy val requiredParams: List[String] = conf.getObject(s"sources.$src.required").keySet().asScala.toList


  val source: DataFrame

  private def addParam(dsr: DataStreamReader, param: String, prefix: String): DataStreamReader = {
    val paramValue = conf.getString(s"$prefix$param")
    dsr.option(param, paramValue)
  }

  def addOptionalParam(dsr: DataStreamReader, param: String): DataStreamReader = addParam(dsr, param, optionParamPrefix)
  def addRequiredParam(dsr: DataStreamReader, param: String): DataStreamReader = addParam(dsr, param, requiredParamPrefix)

  def applyOptParams(dsr: DataStreamReader): DataStreamReader = optionalParams.foldLeft(dsr)(addOptionalParam)
  def applyRequiredParams(dsr: DataStreamReader): DataStreamReader = requiredParams.foldLeft(dsr)(addRequiredParam)
  def applyAllParams(dsr: DataStreamReader): DataStreamReader = applyOptParams(applyRequiredParams(dsr))

}

object Source {
   private [sources] val conf: Config = ConfigFactory.load
}