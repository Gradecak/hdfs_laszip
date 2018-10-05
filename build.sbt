name := "hdfs-laszip"

version := "0.1"

scalaVersion := "2.11.6"

mainClass := Some("com.utils.lastools")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.1",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1"
)
