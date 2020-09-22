-- create table

CREATE EXTERNAL TABLE logging_demo.s3_access_logs_parquet_partition 
LIKE PARQUET 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/requestdate=2020-06-08/requesthour=0/part-00059-359d149e-0a60-4ac5-8367-04d247bfdd79.c000'
PARTITIONED BY (RequestDate STRING, RequestHour INT)
STORED AS PARQUET
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/';
