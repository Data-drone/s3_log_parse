-- create table statement
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

MSCK REPAIR TABLE logging_demo.s3_access_logs_parquet_partition;

-- hive string ops
SELECT key, 
split(key, '/')[0] as `db_system`,
split(key, '/')[1] as `db_zone`,
split(key, '/')[2] as `db_source_system`,
split(key, '/')[3] as `db_folder` 
from logging_demo.s3_access_logs_parquet_partition LIMIT 10;

-- prefixes
SELECT
key,
instr(key, '/', -1) as `stop_pos`,
substr(key, 0, instr(key, '/', -1)) as `prefix`
from logging_demo.s3_access_logs_parquet_partition WHERE requestdate = '2020-06-05';

