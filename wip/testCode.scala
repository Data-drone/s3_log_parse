// testing code for building out some algorithms to process the data

val df = spark.sql("select * from default.s3_access_logs_parquet_partition_6 limit 1000000")

// testing splitting the keys out

val df2 = df
    .withColumn("path_split", split($"key", "/"))
    .withColumn("system", $"path_split".getItem(0))
    .withColumn("zone", $"path_split".getItem(1))
    .withColumn("database", $"path_split".getItem(2))


df2.createOrReplaceTempView("logs")

// this is processing the whole thing... need to see if take works faster I guess
// it needs three stages? - ah it is calculating from df to df2 needs to get all the data
// then do the splits
// cache then take 10
spark.sql("SELECT path_split, system, zone, database from logs LIMIT 10").show()

// checking different folders for min max dates to make sure that we get everything converted properly

val file_to_check="s3a://blaws3logsorganised/datesort/20-07-06/*"
val df = spark.read.text(file_to_check)
import spark.implicits._
val regex_pattern = "^([^\\s]+) ([^\\s]+) \\[(.*?)\\] ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) ([^\\s]+) (\".*?\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\".*?\"|-) (\".*?\"|-|[^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^\\s]+|-,)"

import org.apache.spark.sql.functions._

df3.select(min("requesttimestamp"), max("requesttimestamp")).show()

df3.select("requestdate", "requesthour")

df3.repartition(col("requestdate"), col("requesthour"))
           .write.mode("append")
           .insertInto("default.s3_access_logs_parquet_partition")

// test etl
// extracting bucket stuff

val testDF = df3
            .withColumn("_tmp", split($"key", "/"))
            .withColumn("object", $"_tmp".getItem(-1))
        
