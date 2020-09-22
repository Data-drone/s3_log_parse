-- create table

CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.s3_access_logs_parquet_partition 
LIKE PARQUET 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/requestdate=2020-06-08/requesthour=12/part-00068-bf8a38cf-2d5e-413c-8a9b-0a4b8bd8988f.c000'
PARTITIONED BY (RequestDate STRING, RequestHour INT)
STORED AS PARQUET
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/logs/';

-- reload partitions
ALTER TABLE logging_demo.s3_access_logs_parquet_partition RECOVER PARTITIONS;

-- check data

SELECT * FROM logging_demo.s3_access_logs_parquet_partition LIMIT 10;

-- string split into sub database thingies



SELECT key, 
split_part(key, '/', 1) as 'db_system',
split_part(key, '/', 2) as 'db_zone',
split_part(key, '/', 3) as 'db_source_system',
split_part(key, '/', 4) as 'db_folder' from logging_demo.s3_access_logs_parquet_partition LIMIT 10;

-- examine distincts on different levels
WITH db_systems AS
( SELECT 
split_part(key, '/', 1) as 'db_system'
from logging_demo.s3_access_logs_parquet_partition )
SELECT DISTINCT db_system from db_systems;

-- lets try and work out the prefixes
SELECT
regexp_extract(key, '/([^/]+)$', 0)

SELECT DISTINCT
split_part(key, '/', 1) as 'db_system',
split_part(key, '/', 2) as 'db_zone',
split_part(key, '/', 3) as 'db_source_system',
split_part(key, '/', 4) as 'db_folder',
split_part(key, '/', 5) as 'db_table',
char_length(split_part(key, '/', 5)) as length from logging_demo.s3_access_logs_parquet_partition 
WHERE (requestdate = '2020-06-05' or requestdate = '2020-06-06' or requestdate = '2020-06-06'  )
AND char_length(split_part(key, '/', 5)) < 50;


-- split into requests

-- 