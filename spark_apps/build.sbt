name := "Test Repack"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion  = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)
