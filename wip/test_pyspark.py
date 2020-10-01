# Cloudera CML - testing script

from os.path import join, abspath
import os

from pyspark.sql import SparkSession
from pyspark.sql import Row

warehouse_location = abspath('s3a://cdp-sandbox-default-se/user/brian-test/warehouse')
#.config("spark.sql.warehouse.dir", warehouse_location) \
    
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.hadoop.yarn.resourcemanager.principal",os.getenv("HADOOP_USER_NAME")) \
    .config("spark.yarn.access.hadoopFileSystems","s3a://cdp-sandbox-default-se/user/brian-test") \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.sql("SHOW DATABASES").show()
spark.sql("USE logging_demo")
spark.sql("SHOW TABLES").show()

result = spark.sql("SELECT * FROM s3_access_logs_parquet_partition LIMIT 10")
result.show()


