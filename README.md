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
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/admin/awskeyfile.jceks" \
    target/scala-2.11/test-repack_2.11-1.0-SNAPSHOT.jar

```


## ToDo 

Add code to read the groks direct and explore them

