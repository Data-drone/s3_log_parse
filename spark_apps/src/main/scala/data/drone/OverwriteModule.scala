/*
*   Splitting out the original overwrite module so that we can reuse the repack code with some adjustments
*/
package data.drone

/* Spark app to compress my parquet data down */
import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{to_timestamp, year, month, dayofmonth, col, hour, to_date, regexp_extract, split}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.joda.time.{DateTime, Days}
import org.joda.time.format.DateTimeFormat


object OverwriteModule extends RepackRaw {

    override def transform(readText: String, writeParquet: String, spark: SparkSession): Unit = {

            val df = spark.read.text(readText)

            import spark.implicits._

            val regex_pattern = "^([^\\s]+) ([^\\s]+) \\[(.*?)\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+|-,)"
        
            val df2 = df
                .withColumn("bucketowner", regexp_extract(df("value"), regex_pattern, 1))
                .withColumn("bucket", regexp_extract(df("value"), regex_pattern, 2))
                .withColumn("requestdatetime", regexp_extract(df("value"), regex_pattern, 3))
                .withColumn("remoteip", regexp_extract(df("value"), regex_pattern, 4))
                .withColumn("requester", regexp_extract(df("value"), regex_pattern, 5))
                .withColumn("requestid", regexp_extract(df("value"), regex_pattern, 6))
                .withColumn("operation", regexp_extract(df("value"), regex_pattern, 7))
                .withColumn("key", regexp_extract(df("value"), regex_pattern, 8))
                .withColumn("request", regexp_extract(df("value"), regex_pattern, 9))
                .withColumn("httpstatus", regexp_extract(df("value"), regex_pattern, 10))
                .withColumn("errorcode", regexp_extract(df("value"), regex_pattern, 11))
                .withColumn("bytessent", regexp_extract(df("value"), regex_pattern, 12))
                .withColumn("objectsize", regexp_extract(df("value"), regex_pattern, 13))
                .withColumn("totaltime", regexp_extract(df("value"), regex_pattern, 14))
                .withColumn("turnaroundtime", regexp_extract(df("value"), regex_pattern, 15))
                .withColumn("referrer", regexp_extract(df("value"), regex_pattern, 16))
                .withColumn("useragent", regexp_extract(df("value"), regex_pattern, 17))
                .withColumn("versionid", regexp_extract(df("value"), regex_pattern, 18))
                .withColumn("hostid", regexp_extract(df("value"), regex_pattern, 19))
                .withColumn("sigv", regexp_extract(df("value"), regex_pattern, 20))
                .withColumn("ciphersuite", regexp_extract(df("value"), regex_pattern, 21))
                .withColumn("authtype", regexp_extract(df("value"), regex_pattern, 22))
                .withColumn("endpoint", regexp_extract(df("value"), regex_pattern, 23))
                .withColumn("tlsversion", regexp_extract(df("value"), regex_pattern, 24))
                .drop("value")

            val df3 = df2
                .withColumn("_tmp", split($"request", " "))
                .withColumn("requesturi_operation", $"_tmp".getItem(0))
                .withColumn("requesturi_key", $"_tmp".getItem(1))
                .withColumn("requesturi_httpprotoversion", $"_tmp".getItem(2))
                .withColumn("requesttimestamp", to_timestamp($"requestdatetime", "dd/MMM/yyyy:HH:mm:ss Z"))
                .withColumn("bytessent", col("bytessent").cast(IntegerType))
                .withColumn("objectsize", col("objectsize").cast(IntegerType))
                .withColumn("totaltime", col("totaltime").cast(IntegerType))
                .withColumn("turnaroundtime", col("turnaroundtime").cast(IntegerType))
                .withColumn("requestdate", to_date(col("requesttimestamp")))
                .withColumn("requesthour", hour(col("requesttimestamp")))
                .drop("_tmp")

            // Append mode is really slow in general
            df3.repartition(col("requestdate"), col("requesthour"))
                .write
                .option("maxRecordsPerFile", 2000000)
                .mode(SaveMode.Overwrite)
                .partitionBy("requestdate", "requesthour")
                .parquet(writeParquet)
    }  

}