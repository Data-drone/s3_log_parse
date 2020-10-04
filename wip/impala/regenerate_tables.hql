-- scripts to regenerate tables from the parquet files

-- raw table
CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.s3_access_logs_parquet_partition 
LIKE PARQUET 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/requestdate=2020-06-08/requesthour=12/part-00068-bf8a38cf-2d5e-413c-8a9b-0a4b8bd8988f.c000'
PARTITIONED BY (RequestDate STRING, RequestHour INT)
STORED AS PARQUET
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/';

-- reload partitions
ALTER TABLE logging_demo.s3_access_logs_parquet_partition RECOVER PARTITIONS;



-- formatted table

CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.analysis_s3_logging_by_prefix_second 
LIKE PARQUET 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/analysis_s3_logging_by_prefix_second/requestdate=2020-06-08/000646_0'
PARTITIONED BY (requestdate STRING)
STORED AS PARQUET
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/analysis_s3_logging_by_prefix_second/';

-- reload partitions
ALTER TABLE logging_demo.analysis_s3_logging_by_prefix_second RECOVER PARTITIONS;

-- http code trends


-- this is stored in the hive metastore so will need to be rerun
-- may take a while?
COMPUTE STATS logging_demo.s3_access_logs_parquet_partition;
COMPUTE STATS logging_demo.analysis_s3_logging_by_prefix_second;

