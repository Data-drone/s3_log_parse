from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, split    

### Goal is to parse through the s3 prefixes 
### and get a data structure that can be used for visualisations and also analysis

### this is placeholder for now. We haven't worked out ideal cores / num_exec / mem
spark = SparkSession \
    .builder \
    .appName("S3_Analysis") \
    .config("spark.executor.cores", "2") \
    .config("spark.num.executors", "10") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

### Impala SQL here for convenience
## -- examine the prefix keys and when they were first and last seen
## -- for spark analysis
## CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.key_table
## AS SELECT `key`, min(requestdatetime) as first_seen,
## max(requestdatetime) as last_seen
## FROM logging_demo.s3_access_logs_parquet_partition

## We assume that the hive table already exists from the raw logs
key_data = spark.sql("SELECT `key` FROM logging_demp.key_table")

## Schema to create.
## prefixes all refer to a file.
## a file is the list thing in the prefix (-1 index in the python list once we split)
## All other bits are parents.
## two types

## "Folder" / File
## Note Folder doesn't matter for perf testing but is used for permission models and end user exploration

## Folder has parent attribute
## File too but File can change depending on repacking and drop table / append / repartition write

## we need to split the key then read from back to front.
## write out the folders and files
