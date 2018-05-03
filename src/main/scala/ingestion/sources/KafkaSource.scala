package ingestion.sources

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource extends StreamingSource("kafka") {

  def getSource(spark: SparkSession): DataFrame = {
    //base
    val baseKafkaSource = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("sources.kafka.kafka-bootstrap-servers")) //TODO: handle dots in configuration...

    //required config
    val kafkaSource = applyAllParams(baseKafkaSource)

    kafkaSource
      .load
      .selectExpr(s"CAST(value AS STRING) AS value")

  }

}
