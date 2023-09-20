DROP DATABASE IF EXISTS CITIBIKE_PIPELINES;

-- Switch Context
USE ROLE ACCOUNTADMIN;


--CACHING
--Create the Warehouse
CREATE WAREHOUSE IF NOT EXISTS DATAPIPELINES_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

--- Create the database and grant access to the new role create
CREATE DATABASE IF NOT EXISTS CITIBIKE_PIPELINES;

-- Switch Context
USE CITIBIKE_PIPELINES.PUBLIC;
USE WAREHOUSE DATAPIPELINES_WH;


-- Create the table for Trips
CREATE OR REPLACE TABLE TRIPS
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);


-- Create the stage with the S3 bucket
CREATE or replace STAGE CITIBIKE_PIPELINES.PUBLIC.citibike_trips URL = 's3://snowflake-workshop-lab/citibike-trips-csv/';

list @citibike_trips;


-- Define the file format
create or replace FILE FORMAT CITIBIKE_PIPELINES.PUBLIC.CSV 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 0 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('');
 

alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'LARGE';
copy into trips from @citibike_trips file_format=CSV pattern = '.*.*[.]csv[.]gz';
alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'MEDIUM';

--Run from Cold

SELECT *
FROM CITIBIKE_PIPELINES.PUBLIC.TRIPS;

--Turn off results cache and only use local disk cache

Alter session set use_cached_result = false;

SELECT *
FROM CITIBIKE_PIPELINES.PUBLIC.TRIPS;

--Turn on results cache

Alter session set use_cached_result = true;

SELECT *
FROM CITIBIKE_PIPELINES.PUBLIC.TRIPS;

-- Analyze Warehouse Usage Scanned from Cache

SELECT warehouse_name
  ,COUNT(*) AS query_count
  ,SUM(bytes_scanned) AS bytes_scanned
  ,SUM(bytes_scanned*percentage_scanned_from_cache) AS bytes_scanned_from_cache
  ,SUM(bytes_scanned*percentage_scanned_from_cache) / SUM(bytes_scanned) AS percent_scanned_from_cache
FROM snowflake.account_usage.query_history
WHERE start_time >= dateadd(hour,-1,current_timestamp())
  AND bytes_scanned > 0
GROUP BY 1
ORDER BY 5;

--Performance Optmization
--Analyzing queues
SELECT TO_DATE(start_time) AS date
  ,warehouse_name
  ,SUM(avg_running) AS sum_running
  ,SUM(avg_queued_load) AS sum_queued
FROM snowflake.account_usage.warehouse_load_history
WHERE TO_DATE(start_time) >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2
HAVING SUM(avg_queued_load) > 0;

-- Query Memory Spillage

SELECT query_id, SUBSTR(query_text, 1, 50) partial_query_text, user_name, warehouse_name,
  bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
FROM  snowflake.account_usage.query_history
WHERE (bytes_spilled_to_local_storage > 0
  OR  bytes_spilled_to_remote_storage > 0 )
  AND start_time::date > dateadd('days', -45, current_date)
ORDER BY bytes_spilled_to_remote_storage, bytes_spilled_to_local_storage DESC
LIMIT 10;

-- Create Snowpark optimized warehouses

ALTER WAREHOUSE DATAPIPELINES_WH SUSPEND;
ALTER WAREHOUSE DATAPIPELINES_WH SET WAREHOUSE_TYPE='snowpark-optimized';

--Multiclustering and Resource Monitoring
-- Create multi-clusters

CREATE WAREHOUSE IF NOT EXISTS MULTICLUSTER_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE
    MAX_CLUSTER_COUNT = 4
    ;

SHOW WAREHOUSES;

-- You can add a scaling policy to the multicluster to shift from maximinized to auto-scale
ALTER WAREHOUSE MULTICLUSTER_WH SCALING_POLICY = 'STANDARD'  -- Standard policy
ALTER WAREHOUSE MULTICLUSTER_WH SCALING_POLICY = 'ECONOMY'  -- Economy policy


-- Assign Resource Monitor on Warehouse
create or replace resource monitor limiter with credit_quota=5000
      triggers on 75 percent do notify
           on 100 percent do suspend
           on 110 percent do suspend_immediate;

ALTER WAREHOUSE MULTICLUSTER_WH SET RESOURCE_MONITOR = limiter;

USE WAREHOUSE COMPUTE_WH;


-- Clean resources
DROP WAREHOUSE MULTICLUSTER_WH;
alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'XSMALL';
