package ingestion.util

import ingestion.transformations.TransformationsHandler
import ingestion.util.SourceSinkUtils.{conf, joinableSourcesMap}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.expr

import scala.util.Try

object JoinableSourcesUtils {

  /**
    *
    * @param sourceName name of the source
    * @return the source enriched with its transformations if present.
    */
  def getJoinableSource(sourceName: String): DataFrame = {
    val sourceType = conf.getString(s"sources.joinables.$sourceName.sourceType")
    val transformationsHandler = Try {
      new TransformationsHandler(s"sources.joinables.$sourceName")
    }
    val baseSource = joinableSourcesMap(sourceType)(sourceName)

    if (transformationsHandler.isFailure) baseSource
    else transformationsHandler.get.applyTransformations(baseSource)
  }

  /**
    *
    * @return all the joinable sources joined with the respective conditions
    */
  def getJoinableSources: DataFrame = {

    val joinableSources: List[String] = conf.getStringList("sources.joinables.toJoin").asScala.toList
    val joinableConditions = conf.getStringList("sources.joinables.joinConditions").asScala.toList

    val firstSource = getJoinableSource(joinableSources.head)

    (joinableSources.tail zip joinableConditions)
      .foldLeft(firstSource)((accDF, sourceWithCondition) =>
        accDF.join(
          right = getJoinableSource(sourceWithCondition._1),
          expr(sourceWithCondition._2)
        )
      )
  }
}
