name := "SparkESIngestion"

version := "0.1"

scalaVersion := "2.11.8"

//spark dependencies
val sparkVersion = "2.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
