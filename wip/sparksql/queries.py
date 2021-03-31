from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

# Missing Library for this
#from pyspark_llap.sql.session import HiveWarehouseSession

# .config("spark.yarn.principal", "cm_admin") \
    

spark = SparkSession \
    .builder \
    .appName("PythonSQL") \
    .config("spark.executor.cores", "2") \
    .config("spark.num.executors", "10") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("""SELECT operation, requesthour, key, requestdate, 
    requesttimestamp, turnaroundtime, useragent
    FROM logging_demo.s3_access_logs_hdfs_ext""")

df_splitkeys = spark.sql()
