-- not yet tested
-- initial create statement of the table
-- just copied over from hive script at the moment

CREATE EXTERNAL TABLE IF NOT EXISTS logging_demo.s3_access_logs_parquet_partition(
    BucketOwner STRING,
    Bucket STRING,
    RequestDateTime STRING,
    RemoteIP STRING,
    Requester STRING,
    RequestID STRING,
    Operation STRING,
    Key STRING,
    Request STRING,
    HTTPstatus STRING,
    ErrorCode STRING,
    BytesSent INT,
    ObjectSize INT,
    TotalTime INT,
    TurnAroundTime INT,
    Referrer STRING,
    UserAgent STRING,
    VersionId STRING,
    HostId STRING,
    SigV STRING,
    CipherSuite STRING,
    AuthType STRING,
    EndPoint STRING,
    TLSVersion STRING,
    RequestURI_operation STRING,
    RequestURI_key STRING,
    RequestURI_httpProtoversion STRING,
    RequestTimestamp TIMESTAMP
) PARTITIONED BY (RequestDate STRING, RequestHour STRING )
STORED AS PARQUET
LOCATION 's3a://blaws3logsorganised/dateparquet/test6/';


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
