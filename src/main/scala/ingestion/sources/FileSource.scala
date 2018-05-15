package ingestion.sources

import com.typesafe.config.ConfigValue

import collection.JavaConverters._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

import scala.util.{Success, Try}

class FileSource extends Source("file") {
  import ingestion.Runner.spark
  import Source.conf

  lazy val path: String = conf.getString(s"sources.$src.required.path")
  lazy val format: String = conf.getString(s"sources.$src.required.format")
  lazy val refreshTime: Long = conf.getLong(s"sources.$src.optional.refreshTime")
  lazy val userSchema: List[ConfigValue] = conf.getList(s"sources.$src.schema").asScala.toList

  lazy val source: DataFrame = {
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

    /**
      *
      * @param cv : a lightbend config value object (e.g. : {name=alice} )
      * @return a pair of (nameField, fieldType)
      */
    def formatConfigValue(cv: ConfigValue): (String, String) = {
      val fieldTypePair = cv.unwrapped().toString.replaceAll("[^a-zA-Z =]", "").split("=")
      (fieldTypePair(0), fieldTypePair(1))
    }


    val formattedSchema: Try[List[(String, String)]] = Try {
      userSchema
        .map(formatConfigValue)
    }

    formattedSchema match {
      case Success(schemaFields) => applyCSVSchema(dsr, schemaFields)
      case _ => dsr
    }


  }

}

object FileSource {
  import Source.conf

  def getSourceAsJoinable(sourceName: String): DataFrame = new FileSource with JoinableSource {

    override val src: String = sourceName
    override lazy val userSchema: List[ConfigValue] = conf.getList(s"sources.joinables.$src.schema").asScala.toList
    override lazy val path: String = conf.getString(s"sources.joinables.$src.required.path")
    override lazy val format: String = conf.getString(s"sources.joinables.$src.required.format")
    override lazy val refreshTime: Long = conf.getLong(s"sources.joinables.$src.optional.refreshTime")
  }.source

}