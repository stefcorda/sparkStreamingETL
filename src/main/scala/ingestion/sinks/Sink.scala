package ingestion.sinks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}

import collection.JavaConverters._

abstract class Sink(val snk: String) {

  private[sinks] val conf: Config = ConfigFactory.load

  private[sinks] val optionParamPrefix = s"sinks.$snk.optional."
  private[sinks] val optionalParams: List[String] = conf.getObject(s"sinks.$snk.optional").keySet().asScala.toList
  private[sinks] val requiredParamPrefix = s"sinks.$snk.required."
  private[sinks] val requiredParams: List[String] = conf.getObject(s"sinks.$snk.required").keySet().asScala.toList


  def getSink(df: DataFrame): StreamingQuery

  def addRequiredParam[A](dsw: DataStreamWriter[A], param: String): DataStreamWriter[A] = addParam(dsw, param, requiredParamPrefix)
  def addOptionalParam[A](dsw: DataStreamWriter[A], param: String): DataStreamWriter[A] = addParam(dsw, param, optionParamPrefix)

  private def addParam[A](dsw: DataStreamWriter[A], param: String, prefix: String): DataStreamWriter[A] = {
    val paramValue = conf.getString(s"$prefix$param")
    dsw.option(param, paramValue)
  }

  def applyOptParams[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] = optionalParams.foldLeft(dsw)(addOptionalParam)
  def applyRequiredParams[A](dsw: DataStreamWriter[A]): DataStreamWriter[A] = requiredParams.foldLeft(dsw)(addRequiredParam)
  def applyAllParams[A](dsw: DataStreamWriter[A]): DataStreamWriter[A]= applyOptParams(applyRequiredParams(dsw))


}
