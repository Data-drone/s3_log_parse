-- hive code for creating tables into hive metastore
-- raw data table for the s3 logs
-- assumed to be already parquet packed by the spark app
-- and stored in appropriate folders for hive to run
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

-- regenerate the partitions
MSCK REPAIR TABLE logging_demo.s3_access_logs_parquet_partition;

-- Likely Hive LLAP create analysis table code
-- untested
CREATE EXTERNAL TABLE logging_demo.analysis_s3_logging_by_prefix_second_20200606
PARTITIONED BY (requestdate)
STORED AS PARQUET 
LOCATION 's3a://cdp-sandbox-default-se/user/brian-test/warehouse/analysis' AS
WITH prefix_table as (
    SELECT operation, requesthour, key, requestdate, requesttimestamp, turnaroundtime, useragent,
    substr(key, 0, length(key)-length(split(reverse(key),'[/]')[0]) ) as `prefix`
    FROM logging_demo.s3_access_logs_parquet_partition WHERE requestdate = '2020-06-06'
) 
SELECT requesthour, prefix, operation, useragent, minute(requesttimestamp) as `minute`, 
second(requesttimestamp) as `seconds`, count(*) as `request_count`, avg(turnaroundtime) as `avg_turnaroundtime`, requestdate FROM prefix_table 
GROUP BY requestdate, requesthour, prefix, operation, useragent, minute(requesttimestamp), second(requesttimestamp);


