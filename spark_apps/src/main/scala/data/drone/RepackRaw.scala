// WIP to repack the raw files

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


object RepackRaw {

    def main(args: Array[String]) {

        //jceks file in cluster config not working yet
        // manually setting in spark shell works
        val spark = SparkSession
            .builder()
            .appName("RepackApp")
            .config("spark.executor.cores", "5")
            .config("spark.num.executors", "21")
            .config("spark.executor.memory", "18g")
            .getOrCreate()
        
        // need to have the slash at the end
        // val rawS3Data = "s3a://blaws3logs/"
        // val rawTest = "s3a://blaws3logsorganised/datesort/20-06-05/s3serveraccesslogging-alpha2-prod2020-06-05-09*" 
        // val rawByDate = "s3a://blaws3logsorganised/datesort/20-06-0*"

        // val df = spark.read.text(rawPath+"/*")

        // add code changes to step through days instead of processing ta once?
        process(args(0), args(1), args(2), args(3), spark)
        
        spark.stop()

    }

    def process(readPath: String, writePath: String, 
                    startDate: String, endDate: String, spark: SparkSession): Unit = {

        // date format = yyyy-mm-dd
        val startDateFormat = DateTime.parse(startDate)
        val endDateFormat = DateTime.parse(endDate)

        // this is the folder structure that we have
        val formatter  = DateTimeFormat.forPattern("yy-MM-dd")
        
        if (startDateFormat == endDateFormat) {
            val reformatDate = startDateFormat.toString(formatter)
            val readDir = readPath + reformatDate

            transform(readDir, writePath, spark)
        } else {

            val daysCount = Days.daysBetween(startDateFormat, endDateFormat).getDays()
            for (i <- 0 until daysCount) {
                val x = startDateFormat.plusDays(i)
                val reformatDate = x.toString(formatter)
                val readDir = readPath + reformatDate
                transform(readDir, writePath, spark)
            }

        }

    }

    def transform(readText: String, writeParquet: String, spark: SparkSession): Unit = {

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

            df3.repartition(col("requestdate"))
                .write
                .option("maxRecordsPerFile", 2000000)
                .mode(SaveMode.Append)
                .partitionBy("requestdate", "requesthour")
                .parquet(writeParquet)

    }  

}