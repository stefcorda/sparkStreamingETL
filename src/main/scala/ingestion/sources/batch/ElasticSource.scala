package ingestion.sources.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._

object ElasticSource extends StaticSource("es") {

  val indexName: String = conf.getString(s"$sourcePrefix.index")
  val logType: String = conf.getString(s"$sourcePrefix.type")

  val selectedFields: List[String] = conf.getString(s"$sourcePrefix.columns").split(",").toList
  val filterQuery: String = conf.getString(s"$sourcePrefix.filter")

  override def getSource(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val baseEsDF = spark
      .read
      .format("org.elasticsearch.spark.sql")

    val enrichedEsDF = applyAllParams(baseEsDF)

    enrichedEsDF
      .load(s"$indexName/$logType")
      .selectExpr(selectedFields: _*)
      .where(filterQuery)

  }


}
