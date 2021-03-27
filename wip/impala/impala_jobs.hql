-- some jobs to run using in order to load up impala

SELECT count(*) from s3_access_logs_parquet_partition \
WHERE hour(now()) = hour(requesttimestamp) \
AND minute(now()) = minute(requesttimestamp)