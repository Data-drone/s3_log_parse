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


object RepackRaw {

    def main(args: Array[String]) {

        //jceks file in cluster config not working yet
        // manually setting in spark shell works
        val spark = SparkSession
            .builder()
            .appName("RepackApp")
            .config("spark.executor.cores", "1")
            .config("spark.num.executors", "70")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        
        import spark.implicits._

        // need to have the slash at the end
        // val rawS3Data = "s3a://blaws3logs/"
        val rawPath = args(0)
        // val rawTest = "s3a://blaws3logsorganised/datesort/20-06-05/s3serveraccesslogging-alpha2-prod2020-06-05-09*" 
        // val rawByDate = "s3a://blaws3logsorganised/datesort/20-06-0*"

        // val df = spark.read.text(rawPath+"/*") 
        val df = spark.read.text(rawPath)

        //val regex_pattern = "([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$"
        //val regex_pattern_2 = "^([^\\s]+) ([^\\s]+) \\[.*?\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) \".*?\" \".*?\" ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+)".r
        
        // think pattern isn't working cause of delete commands!
        // val regex_pattern = "^([^\s]+) ([^\s]+) \[.*?\] ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) (".*?"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (".*?"|-) (".*?"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\s]+)"
        // val fix_s3 = "^([^\s]+) ([^\s]+) \[.*?\] ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) (".*?"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (".*?"|-) (".*?"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\s]+|-,)"

        val regex_pattern = "^([^\\s]+) ([^\\s]+) \\[(.*?)\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+|-,)"
        // val regex_pattern_f = "^([^\\s]+) ([^\\s]+) \\[.*?\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+)"
        // Regex in scala is slightly different from the hiveserde
        // might need to consider switching to hiveql and using the serde properly later

        // need to check nulls
        // df.filter( (df("value") === null) || (df("value") === "") ).count()

        val df2 = df
            .withColumn("bucketowner", regexp_extract(df("value"), regex_pattern, 1))
            .withColumn("vucket", regexp_extract(df("value"), regex_pattern, 2))
            .withColumn("requestdatetime", regexp_extract(df("value"), regex_pattern, 3))
            .withColumn("remoteip", regexp_extract(df("value"), regex_pattern, 4))
            .withColumn("requester", regexp_extract(df("value"), regex_pattern, 5))
            .withColumn("requestid", regexp_extract(df("value"), regex_pattern, 6))
            .withColumn("operation", regexp_extract(df("value"), regex_pattern, 7))
            .withColumn("key", regexp_extract(df("value"), regex_pattern, 8))
            .withColumn("eequest", regexp_extract(df("value"), regex_pattern, 9))
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

        // quick check
        //df2.filter( (df2("RequestDateTime") === null) || ( df2("RequestDateTime") === ""  )  ).count()


        val df3 = df2
            .withColumn("_tmp", split($"Request", " "))
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
            //.orderBy(col("RequestTimestamp"))
            // I think order is a really expensive operation

        // test code
        // df3.filter( (df3("RequestDateTime") === null) || ( df3("RequestDateTime") === ""  )  ).count()
        // df3.createOrReplaceTempView("first_day")
        // 

        // group by hr and save
        val write_path = args(1)
        // val write_path = "s3a://blaws3logsorganised/dateparquet/test2/"
        df3.write.mode(SaveMode.Append).partitionBy("requestDate", "requesthour").parquet(write_path)

        spark.stop()

    }

}