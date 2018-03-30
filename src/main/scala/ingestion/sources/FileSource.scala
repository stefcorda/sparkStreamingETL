package ingestion.sources

import com.typesafe.config.{Config, ConfigFactory}

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
    val userSchema: Try[List[String]] = Try {
      conf.getObject(s"sources.$src.schema").keySet().asScala.toList
    }

    userSchema match {
      case Success(schemaFields) => applyCSVSchema(dsr, schemaFields)
      case _ => dsr
    }
  }


  private def applyCSVSchema(dsr: DataStreamReader, schemaFields: List[String]): DataStreamReader = {
    val userSchema = schemaFields
      .foldLeft(new StructType())((struct, fieldName) => struct.add(fieldName, conf.getString(s"sources.$src.schema.$fieldName")))

    dsr.schema(userSchema)
  }


}
