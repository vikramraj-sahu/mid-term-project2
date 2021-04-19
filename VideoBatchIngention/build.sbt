name := "VideoBatchIngestion"

version := "0.1"

scalaVersion := "2.11.12"

// mainClass in (Compile, run) := Some("WebsiteBatchIngestion")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "com.databricks" %% "spark-xml" % "0.5.0",
  "mysql" % "mysql-connector-java" % "5.1.38"
)