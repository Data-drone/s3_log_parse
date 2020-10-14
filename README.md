# Parsing S3 Access Logs with Spark

Test scripts to parse S3 Access Logs

Installing sbt on edge-node
https://linuxadminonline.com/how-to-install-sbt-on-centos-7/


# Authentication on Clusters

For CDH 5-6:

- Setting up the jceks file
(need to make sure that we can create the folder and have permissions into it first)
Follow this: 
https://community.cloudera.com/t5/Support-Questions/Setup-Keystore-for-AWS-Keys/td-p/51436

```{bash}

hadoop credential create fs.s3a.access.key -provider jceks://hdfs/user/cm_admin/awskeyfile.jceks -value <aws_access_id>

hadoop credential create fs.s3a.secret.key -provider jceks://hdfs/user/cm_admin/awskeyfile.jceks -value <aws_secret_key>

```

For CDP-PvC

Setup Knox aws_idbroker mappings
Need to map Impala user as well for hue impala
untested yet for hue - hive-on-tez

Trust Relationship for AWS IAM Role assuming that we added a user's role into the knox aws creds
```{json}

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::XXXXXXX:user/whoever"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

- Building

```{bash}

sbt clean package

```

- Spark Shell

```{bash}

spark-shell \
    --driver-cores 4 \
    --driver-memory 10G \
    --conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/cm_admin/awskeyfile.jceks"

```

- Spark Submit

SmartOverwrite appends to an existing table so need to make sure that the table already exists


```{bash}
--conf spark.hadoop.hadoop.security.credential.provider.path="jceks://hdfs/user/cm_admin/awskeyfile.jceks" \
    
    --principal cm_admin \
    --keytab cm_admin.keytab \

spark-submit \
    --class data.drone.SmartOverwrite \
    --deploy-mode cluster \
    --master yarn \
    --driver-cores 6 \
    --driver-memory 16G \
    --executor-cores 6 \
    --executor-memory 16G \
    --conf spark.driver.maxResultSize=16G \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.speculation=false \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    target/scala-2.11/test-repack_2.11-1.0-SNAPSHOT.jar \
    "s3a://blaws3logsorganised/datesort/" \
    "default.s3_access_logs_parquet_partition" \
    "2020-07-01" \
    "2020-07-08"
```

2020-07-07 2020-07-20


## ToDo 

Add code to read the groks direct and explore them

