package ingestion.transformations

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import collection.JavaConverters._

object ColumnsHandler {

  private[this] val conf: Config = ConfigFactory.load

  def removeColumns(df: DataFrame): DataFrame = {
    val columnsToRemove: List[String] = conf.getStringList("transformations.remove.columns").asScala.toList
    df.drop(columnsToRemove: _*)
  }

  def addColumns(df: DataFrame): DataFrame = {

    def getColValue(key: String): String = conf.getString(s"transformations.add.columns.$key.value")

    case class ColInfo(name: String, value: String)

    val columnsInfo: List[ColInfo] = conf
      .getObject("transformations.add.columns").keySet().asScala.toList
      .map(column => ColInfo(column, getColValue(column)))

    columnsInfo.foldLeft(df)(
      (augmentedDF: DataFrame, colInfo) => augmentedDF.withColumn(colInfo.name, lit(colInfo.value))
    )
  }

}
