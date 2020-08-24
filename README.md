# Parsing S3 Access Logs with Spark

Test scripts to parse S3 Access Logs


- Building

```{bash}

sbt clean package

```

- Spark Shell

```{bash}

spark-shell \
    --driver-cores 4 \
    --driver-memory 10G \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/admin/awskeyfile.jceks"

```

- Spark Submit

```{bash}

spark-submit \
    --class data.drone.SmartOverwrite \
    --deploy-mode cluster \
    --master yarn \
    --driver-cores 4 \
    --driver-memory 10G \
    --executor-cores 4 \
    --executor-memory 10G \
    --conf spark.driver.maxResultSize=9G \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.speculation=false \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/admin/awskeyfile.jceks" \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    target/scala-2.11/test-repack_2.11-1.0-SNAPSHOT.jar \
    "s3a://blaws3logsorganised/datesort/" \
    "default.s3_access_logs_parquet_partition_6" \
    "2020-06-05" \
    "2020-07-07"
```

2020-07-07 2020-07-20


## ToDo 

Add code to read the groks direct and explore them

