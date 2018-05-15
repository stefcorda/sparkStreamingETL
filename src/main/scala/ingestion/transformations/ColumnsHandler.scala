package ingestion.transformations

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import collection.JavaConverters._

class ColumnsHandler(prefixPath: String) {

  private[this] val conf: Config = ConfigFactory.load

  def removeColumns(df: DataFrame): DataFrame = {
    val columnsToRemove: List[String] = conf.getStringList(s"$prefixPath.remove.columns").asScala.toList
    df.drop(columnsToRemove: _*)
  }

  def addColumns(df: DataFrame): DataFrame = {

    def getColValue(key: String): String = conf.getString(s"$prefixPath.add.columns.$key.value")

    case class ColInfo(name: String, value: String)

    val columnsInfo: List[ColInfo] = conf
      .getObject(s"$prefixPath.add.columns").keySet().asScala.toList
      .map(column => ColInfo(column, getColValue(column)))

    columnsInfo.foldLeft(df)(
      (augmentedDF: DataFrame, colInfo) => augmentedDF.withColumn(colInfo.name, lit(colInfo.value))
    )
  }

  def filterOnColumns(df: DataFrame): DataFrame = {
    val filterConditions: List[String] = conf.getStringList(s"$prefixPath.filter").asScala.toList
    filterConditions.foldLeft(df)((df, filterCond) => df.filter(filterCond))
  }

  def selectOnColumns(df: DataFrame): DataFrame = {
    val selectConditions: List[String] = conf.getStringList(s"$prefixPath.select").asScala.toList

    df.selectExpr(selectConditions: _*)
  }

}
