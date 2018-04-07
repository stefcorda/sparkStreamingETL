import com.typesafe.config.{ConfigFactory, ConfigObject}


import collection.JavaConverters._

object Runner2 {


  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load


    def getType(key: String): String = conf.getString(s"transformations.add.columns.$key.type")

    def getValue(key: String): String = conf.getString(s"transformations.add.columns.$key.value")


    val transformationsToApply = conf.getObject(s"transformations").toString

    println (transformationsToApply )

  }
}
