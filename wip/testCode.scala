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