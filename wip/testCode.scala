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
        

// rapids testing
// rapids doesn't support flat textformat / timestamp / regex
// /opt/rapids/getGpusResources.sh
// checkout 1 day
//val check_dirs="s3a://blaws3logsorganised/dateparquet/test6/requestdate=2020-06-19/*"
val check_dirs="s3a://blaw-files/s3logs_parquet/datalake/raws3logs_fix1/requestdate=2020-07-01/requesthour=11/part-00047-19bbf8cd-9844-4595-a9ef-805c19920a16-application_1602251761531_0003.c000"
val df = spark.read.parquet(check_dirs)

val df2 = df
    .withColumn("_tmp", split($"key", "/"))
    .withColumn("storage_area", $"_tmp".getItem(0))
    .withColumn("zone", $"_tmp".getItem(1))
    .withColumn("database", $"_tmp".getItem(2))
    .withColumn("database_area", $"_tmp".getItem(3))

df2.filter(!col("zone").isin("edh_fs_config"))
   .select("storage_area", "zone", "database", "database_area").distinct.show(40, false)

df2.filter(col("zone") === "conformed").select("database", "database_area").distinct.show(40, false)

df2.filter(col("zone") === "conformed").select("key").distinct.show(40, false)

df2.filter(col("operation").isin("REST.PUT.OBJECT", "REST.GET.OBJECT"))
    .groupBy("zone", "operation")
    .agg(avg("objectsize"),
        avg("turnaroundtime"),
        min("turnaroundtime"),
        max("turnaroundtime"),
        variance("turnaroundtime"),
        count("*")).orderBy("zone", "operation").show(40,false)

df2.createOrReplaceTempView("output")

df.filter(df("operation") === "REST.GET.OBJECT")
  .groupBy("key")
  .agg(avg("objectsize"),
        avg("turnaroundtime"),
        min("turnaroundtime"),
        max("turnaroundtime")).show(40,false)

