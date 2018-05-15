package ingestion.sources

import ingestion.transformations.TransformationsHandler
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._


trait JoinableSource extends Source {
  import Source.conf

  override private[sources] lazy val optionParamPrefix = s"sources.joinables.$src.optional."
  override private[sources] lazy val optionalParams: List[String] = conf.getObject(s"sources.joinables.$src.optional").keySet().asScala.toList
  override private[sources] lazy val requiredParamPrefix = s"sources.joinables.$src.required."
  override private[sources] lazy val requiredParams: List[String] = conf.getObject(s"sources.joinables.$src.required").keySet().asScala.toList

  def transformationsBeforeJoin(df: DataFrame): DataFrame = {
    val transformations: List[String] = conf.getStringList(s"sources.joinables.$src.transformations").asScala.toList

    new TransformationsHandler(s"sources.joinables.$src").applyTransformations(df, transformations)
  }
}
