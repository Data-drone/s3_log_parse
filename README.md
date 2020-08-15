# Parsing S3 Access Logs with Spark

Test scripts to parse S3 Access Logs


- Building

```{bash}

sbt clean package

```

- Spark Submit

```{bash}

spark-submit \
    --class data.drone.RepackRaw \
    --deploy-mode cluster \
    --master yarn \
    --driver-cores 4 \
    --driver-memory 20G \
    --conf spark.driver.maxResultSize=16G \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/admin/awskeyfile.jceks" \
    target/scala-2.11/test-repack_2.11-1.0-SNAPSHOT.jar \
    "s3a://blaws3logsorganised/datesort/20-06-*/*" \
    "s3a://blaws3logsorganised/dateparquet/test3/" \
    "2020-07-01" \
    "2020-07-03"

```


## ToDo 

Add code to read the groks direct and explore them

