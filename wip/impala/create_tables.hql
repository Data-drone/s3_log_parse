-- not yet tested
-- initial create statement of the table
-- just copied over from hive script at the moment

CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.s3_access_logs_parquet_partition(
    bucketowner STRING,
    bucket STRING,
    requestdatetime STRING,
    remoteip STRING,
    requester STRING,
    requestid STRING,
    operation STRING,
    key STRING,
    request STRING,
    httpstatus STRING,
    errorcode STRING,
    bytessent INT,
    objectsize INT,
    totaltime INT,
    turnaroundtime INT,
    referrer STRING,
    useragent STRING,
    versionid STRING,
    hostid STRING,
    sigv STRING,
    ciphersuite STRING,
    authtype STRING,
    endpoint STRING,
    tlsversion STRING,
    requesturi_operation STRING,
    requesturi_key STRING,
    requesturi_httpprotoversion STRING,
    requesttimestamp TIMESTAMP
) PARTITIONED BY (requestdate STRING, requesthour STRING )
STORED AS PARQUET
LOCATION 's3a://blaw-files/s3logs_parquet/datalake/raws3logs/';
-- 's3a://blaws3logsorganised/dateparquet/fix1/'

-- update partitions


-- create a table to look at prefix behaviour
-- this blows up the cache in impala
-- nbeed hive
CREATE EXTERNAL TABLE logging_demo.analysis_s3_logging_by_prefix_second
PARTITIONED BY (requestdate)
STORED AS PARQUET 
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/analysis_s3_logging_by_prefix_second' AS
WITH 'prefix_table' as (
    SELECT operation, requesthour, key, requestdate, requesttimestamp, turnaroundtime, useragent,
    STRLEFT(key, instr(key, '/', -1)) as 'prefix'
    FROM logging_demo.s3_access_logs_parquet_partition
) 
SELECT requesthour, prefix, operation, useragent, minute(requesttimestamp) as 'minute', 
second(requesttimestamp) as 'seconds', count(*) as 'request_count', avg(turnaroundtime) as 'avg_turnaroundtime', requestdate FROM prefix_table 
GROUP BY requestdate, requesthour, prefix, operation, useragent, minute(requesttimestamp), second(requesttimestamp);


-- examining HTTP Code Trends

CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.analysis_http_codes_per_hour
PARTITIONED BY (requestdate)
STORED AS PARQUET 
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/analysis_http_codes_per_hour' AS
SELECT requesttimestamp, useragent, httpstatus, count(*) as 'event_count', requestdate FROM s3_access_logs_parquet_partition
GROUP BY requestdate, requesttimestamp, useragent, httpstatus;

-- examine the prefix keys and when they were first and last seen
-- for spark analysis
CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.key_table
AS SELECT `key`, min(requestdatetime) as first_seen,
max(requestdatetime) as last_seen
FROM logging_demo.s3_access_logs_parquet_partition

