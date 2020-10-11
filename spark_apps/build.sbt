name := "Test Repack"
version := "1.0-SNAPSHOT"
organization := "data.drone"

scalaVersion := "2.12.10"

val sparkVersion  = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion % Provided,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion % Provided,
  "org.apache.spark" % "spark-hive_2.12" % sparkVersion % Provided
)