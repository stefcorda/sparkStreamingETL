package ingestion.sources

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}

import collection.JavaConverters._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

object FileSource extends Source("file") {

  private[this] val conf: Config = ConfigFactory.load

  val path: String = conf.getString(s"sources.$src.required.path")
  val format: String = conf.getString(s"sources.$src.required.format")
  val refreshTime: Long = conf.getLong(s"sources.$src.optional.refreshTime")

  override def getSource(spark: SparkSession): DataFrame = {
    val baseSource: DataStreamReader = spark
      .readStream
      .format(format)

    val fileSource = applyAllParams(baseSource)

    val sourceWithSchema = checkAndApplySchema(fileSource)

    sourceWithSchema.load(path)

  }

  /**
    *
    * @param dsr : The DataStreamReader
    * @return The DataStreamReader with its associated schema if present, the original DataStreamReader otherwise
    */
  private def checkAndApplySchema(dsr: DataStreamReader): DataStreamReader = {

    val userSchema: Try[List[(String, String)]] = Try {
      conf
        .getList(s"sources.$src.schema")
        .asScala.toList
        .map(formatConfigValue)
    }

    userSchema match {
      case Success(schemaFields) => applyCSVSchema(dsr, schemaFields)
      case _ => dsr
    }

    /**
      *
      * @param cv : a lightbend config value object (e.g. : {name=alice} )
      * @return a pair of (nameField, fieldType)
      */
    def formatConfigValue(cv: ConfigValue): (String, String) = {
      val fieldTypePair = cv.unwrapped().toString.replaceAll("[^a-zA-Z =]", "").split("=")
      (fieldTypePair(0), fieldTypePair(1))
    }

    /**
      *
      * @param dsr          the datastream reader to apply schema to
      * @param schemaFields the pair list (fieldName, fieldType) to apply
      * @return the datastream with the given schema applied
      */
    def applyCSVSchema(dsr: DataStreamReader, schemaFields: List[(String, String)]): DataStreamReader = {
      val userSchema = schemaFields
        .foldLeft(new StructType())((struct, fieldPair) => struct.add(fieldPair._1, fieldPair._2))

      dsr.schema(userSchema)
    }

  }

}
