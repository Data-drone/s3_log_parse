-- impala sql
-- check data

SELECT * FROM logging_demo.s3_access_logs_parquet_partition LIMIT 10;

-- string split into sub database thingies

SELECT key, 
split_part(key, '/', 1) as 'db_system',
split_part(key, '/', 2) as 'db_zone',
split_part(key, '/', 3) as 'db_source_system',
split_part(key, '/', 4) as 'db_folder' from logging_demo.s3_access_logs_parquet_partition LIMIT 10;

-- get prefixes
SELECT
key,
instr(key, '/', -1) as 'stop_pos',
STRLEFT(key, instr(key, '/', -1)) as 'prefix'
from logging_demo.s3_access_logs_parquet_partition WHERE requestdate = '2020-06-05';

-- building prefix exploration table
SELECT operation, requesthour, key, requestdate,
    STRLEFT(key, instr(key, '/', -1)) as 'prefix'
    FROM logging_demo.s3_access_logs_parquet_partition;


-- lets look for bottlenecking
-- prefix analysis
CREATE VIEW IF NOT EXISTS logging_demo.prefix_stats
AS 
WITH 'prefix_table' as (
    SELECT operation, requesthour, key, requestdate, requesttimestamp,
    bytessent,
    STRLEFT(key, instr(key, '/', -1)) as 'prefix'
    FROM logging_demo.s3_access_logs_parquet_partition
) 
SELECT requesthour, prefix, operation, minute(requesttimestamp) as 'requestminute', 
second(requesttimestamp) as 'requestsecond', count(*) as 'num_simultaneous_requests' 
, avg(bytessent) FROM prefix_table 
GROUP BY requesthour, prefix, operation, minute(requesttimestamp), second(requesttimestamp);

SELECT * FROM prefix_stats
WHERE num_queries > 5500;

-- I dunno what endpoint represents?


-- examine distincts on different levels
-- something goofy there
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


-- looking at user agent:
SELECT requestdate, requesthour, count(*) 
FROM logging_demo.s3_access_logs_parquet_partition 
WHERE useragent = '"snowflake/1.0"' AND requestdate in ('2020-06-15', '2020-06-18', '2020-06-25', '2020-07-01', '2020-07-15')
GROUP BY requestdate, requesthour
ORDER BY requestdate, requesthour;


-- Exploring the Request ID
-- 79F16D767C296F74 on the 2020-07-01

SELECT requestid, requestdatetime, key, objectsize, useragent, referrer FROM logging_demo.s3_access_logs_parquet_partition
WHERE requestid = '79F16D767C296F74';

-- explore the prefixes and the dependencies
CREATE TABLE logging_demo.key_table
AS SELECT key, min(requestdatetime), max(requestdatetime)
FROM logging_demo.s3_access_logs_parquet_partition
GROUP BY key

-- we will have to explore key depths in spark so that we can look at the key splits etc
-- explore the key depths
