package ingestion.sources

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource extends Source("kafka") {

  def getSource(spark: SparkSession): DataFrame = {

    //base
    val baseKafkaSource = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") //TODO: handle dots in configuration...

    //required config
    val kafkaSource = applyAllParams(baseKafkaSource)

    kafkaSource.load
  }

}
