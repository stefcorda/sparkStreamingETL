name := "SparkESIngestion"

version := "0.1"

scalaVersion := "2.11.8"

//avoiding conflicts with spark dependencies
val jacksonVersion = "2.8.9"
dependencyOverrides ++= Seq("com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonVersion
)
dependencyOverrides += "net.jpountz.lz4" % "lz4" % "1.3.0"

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

val esVersion = "6.2.0"

//dependency for ES integration with spark
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % esVersion

//config library
libraryDependencies += "com.typesafe" % "config" % "1.3.2"