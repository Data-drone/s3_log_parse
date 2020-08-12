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


object RepackRaw {

    def main(args: Array[String]) {

        //jceks file in cluster config not working yet
        // manually setting in spark shell works
        val spark = SparkSession
            .builder()
            .appName("RepackApp")
            .config("spark.executor.cores", "2")
            .config("spark.num.executors", "15")
            .config("spark.executor.memory", "3g")
            .getOrCreate()
        
        import spark.implicits._

        // need to have the slash at the end
        // val rawS3Data = "s3a://blaws3logs/"
        //val rawPath = args(0)
        // val rawTest = "s3a://blaws3logsorganised/datesort/20-06-05/s3serveraccesslogging-alpha2-prod2020-06-05-09*" 
        val rawByDate = "s3a://blaws3logsorganised/datesort/20-06-05/"

        // val df = spark.read.text(rawPath+"/*") 
        val df = spark.read.text(rawByDate+"/*")

        //val regex_pattern = "([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\") ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$"
        //val regex_pattern_2 = "^([^\\s]+) ([^\\s]+) \\[.*?\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) \".*?\" \".*?\" ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+)".r
        
        // think pattern isn't working cause of delete commands!
        // val regex_pattern = "^([^\s]+) ([^\s]+) \[.*?\] ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) (".*?"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (".*?"|-) (".*?"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\s]+)"
        // val fix_s3 = "^([^\s]+) ([^\s]+) \[.*?\] ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) ([^\s]+) (".*?"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (".*?"|-) (".*?"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\s]+|-,)"

        val regex_pattern = "^([^\\s]+) ([^\\s]+) \\[.*?\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+|-,)"
        // val regex_pattern_f = "^([^\\s]+) ([^\\s]+) \\[.*?\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+)"
        // Regex in scala is slightly different from the hiveserde
        // might need to consider switching to hiveql and using the serde properly later

        // need to check nulls
        // df.filter( (df("value") === null) || (df("value") === "") ).count()

        val df2 = df
            .withColumn("BucketOwner", regexp_extract(df("value"), regex_pattern, 1))
            .withColumn("Bucket", regexp_extract(df("value"), regex_pattern, 2))
            .withColumn("RequestDateTime", regexp_extract(df("value"), regex_pattern, 3))
            .withColumn("RemoteIP", regexp_extract(df("value"), regex_pattern, 4))
            .withColumn("Requester", regexp_extract(df("value"), regex_pattern, 5))
            .withColumn("Operation", regexp_extract(df("value"), regex_pattern, 6))
            .withColumn("Key", regexp_extract(df("value"), regex_pattern, 7))
            .withColumn("Request", regexp_extract(df("value"), regex_pattern, 8))
            //.withColumn("RequestURI_operation", regexp_extract(df("value"), regex_pattern, 8))
            //.withColumn("RequestURI_key", regexp_extract(df("value"), regex_pattern, 9))
            //.withColumn("RequestURI_httpProtoversion", regexp_extract(df("value"), regex_pattern, 11))
            .withColumn("HTTPstatus", regexp_extract(df("value"), regex_pattern, 9))
            .withColumn("ErrorCode", regexp_extract(df("value"), regex_pattern, 10))
            .withColumn("BytesSent", regexp_extract(df("value"), regex_pattern, 11))
            .withColumn("ObjectSize", regexp_extract(df("value"), regex_pattern, 12))
            .withColumn("TotalTime", regexp_extract(df("value"), regex_pattern, 13))
            .withColumn("TurnAroundTime", regexp_extract(df("value"), regex_pattern, 14))
            .withColumn("Referrer", regexp_extract(df("value"), regex_pattern, 15))
            .withColumn("UserAgent", regexp_extract(df("value"), regex_pattern, 16))
            .withColumn("VersionId", regexp_extract(df("value"), regex_pattern, 17))
            .withColumn("HostId", regexp_extract(df("value"), regex_pattern, 18))
            .withColumn("SigV", regexp_extract(df("value"), regex_pattern, 19))
            .withColumn("CipherSuite", regexp_extract(df("value"), regex_pattern, 20))
            .withColumn("AuthType", regexp_extract(df("value"), regex_pattern, 21))
            .withColumn("EndPoint", regexp_extract(df("value"), regex_pattern, 22))
            .withColumn("TLSVersion", regexp_extract(df("value"), regex_pattern, 23))
            .drop("value")

        //val df = spark.read.text(read_filter)

        val df3 = df2
            .withColumn("_tmp", split($"Request", " "))
            .withColumn("RequestURI_operation", $"_tmp".getItem(0))
            .withColumn("RequestURI_key", $"_tmp".getItem(1))
            .withColumn("RequestURI_httpProtoversion", $"_tmp".getItem(2))
            .withColumn("RequestTimestamp", to_timestamp($"RequestDateTime", "dd/MMM/yyyy:HH:mm:ss Z"))
            .withColumn("BytesSent", col("BytesSent").cast(IntegerType))
            .withColumn("ObjectSize", col("ObjectSize").cast(IntegerType))
            .withColumn("TotalTime", col("TotalTime").cast(IntegerType))
            .withColumn("TurnAroundTime", col("TurnAroundTime").cast(IntegerType))
            .withColumn("RequestDate", to_date(col("RequestTimestamp")))
            .withColumn("RequestHour", hour(col("RequestTimestamp")))
            .drop("_tmp")
            .orderBy(col("RequestTimestamp"))

        // test code
        // df3.createOrReplaceTempView("first_day")
        // 

        // group by hr and save
        val write_path = "s3a://blaws3logsorganised/dateparquet/test1/"
        df3.write.partitionBy("RequestDate", "RequestHour").parquet(write_path)

        spark.stop()

    }

}