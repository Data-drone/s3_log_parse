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
    --driver-memory 11G \
    --executor-cores 4 \
    --executor-memory 11G \
    --num-executors 20 \
    --conf spark.driver.maxResultSize=10G \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.speculation=false \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/admin/awskeyfile.jceks" \
    target/scala-2.11/test-repack_2.11-1.0-SNAPSHOT.jar \
    "s3a://blaws3logsorganised/datesort/" \
    "hdfs:///user/admin/dateparquet/test4/" \
    "2020-06-15" \
    "2020-06-20"
```
"06-09 - 06-09" need to run - done
"06-12 - 06-12" need to run - done
"06-13 - 06-15" done
"06-15 - 06-20" is the next bit - running with hdfs version to test

We are up to the 15th
Lets maybe kill adjust to write to hdfs then use nifi to copy partitions over?


## ToDo 

Add code to read the groks direct and explore them

