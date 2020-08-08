name := "Test Repack"
version := "1.0-SNAPSHOT"
organization := "data.drone"

scalaVersion := "2.11.12"

val sparkVersion  = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % Provided,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % Provided,
  "org.apache.spark" % "spark-hive_2.11" % sparkVersion % Provided,
  "org.scalatest" % "scalatest_2.11" % "3.2.1" % "test, it"
)