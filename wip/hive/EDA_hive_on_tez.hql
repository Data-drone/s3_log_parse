

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

-- HIVE LLAP
-- prefixes
SELECT
key, length(split(reverse(key),'[/]')[0]),
--length(key)-length((split(reverse(key),'[/]')[0])
substr(key, 0, length(key)-length(split(reverse(key),'[/]')[0]) )
from s3_access_logs_parquet_partition WHERE requestdate = '2020-06-05' LIMIT 10;

-- timestamp conversions
SELECT 
date(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane')) as `brisbane_date`, 
hour(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane'))  as `brisbane_hr`,
FROM s3_access_logs_parquet_partition
WHERE requestdate = '2020-07-01';


-- exploring trends within a day
-- grouping by brisbane hour
SELECT 
date(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane')) as `brisbane_date`, 
hour(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane')) as `brisbane_hr`,
httpstatus, useragent, request,
count(*) as `event_count`
FROM s3_access_logs_parquet_partition
WHERE requestdate = '2020-07-01'
GROUP BY date(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane')), 
hour(from_utc_timestamp(requesttimestamp, 'Australia/Brisbane')),
httpstatus, useragent, request;

