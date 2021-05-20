from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, split, udf, size, element_at, explode, when, lit
import pyspark.sql.functions as F

# this process is just to subset the data into a smaller subset 
# for easy checking of processes

path = '/opt/spark-data'

spark = SparkSession \
    .builder \
    .appName("S3_Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "8") \
    .config("spark.executor.memory", "16g") \
    .config("spark.num.executors", "2") \
    .enableHiveSupport() \
    .getOrCreate()

s3_stats = spark.read.parquet(os.path.join(path, "s3logs"))
s3_stats.createOrReplaceTempView("s3_stats")

subset = spark.sql("SELECT * FROM s3_stats LIMIT 100000")

subset.write.mode("overwrite").parquet(os.path.join(path, "small_logs"))

spark.stop()