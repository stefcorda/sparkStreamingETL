package ingestion.sources

import org.apache.spark.sql.DataFrame

class KafkaSource extends Source("kafka") {

  import ingestion.Runner.spark
  import Source.conf

  lazy val bootstrapServers: String = conf.getString("sources.kafka.kafka-bootstrap-servers")

  override lazy val source: DataFrame = {
    //base
    val baseKafkaSource = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)

    //required config
    val kafkaSource = applyAllParams(baseKafkaSource)

    kafkaSource
      .load
      .selectExpr(s"CAST(value AS STRING) AS value")

  }
}

object KafkaSource {
  import Source.conf

  def getSourceAsJoinable(sourceName: String): DataFrame = new KafkaSource() with JoinableSource {

    override val src: String = sourceName
    override lazy val bootstrapServers: String = conf.getString(s"sources.joinables.$src.kafka-bootstrap-servers")
  }.source

}
