-- Switch Context
USE ROLE ACCOUNTADMIN;

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
CREATE OR REPLACE TABLE trips
(
    tripduration              INTEGER,
    starttime                 TIMESTAMP,
    stoptime                  TIMESTAMP,
    start_station_id          INTEGER,
    start_station_name        STRING,
    start_station_latitude    FLOAT,
    start_station_longitude   FLOAT,
    end_station_id            INTEGER,
    end_station_name          STRING,
    end_station_latitude      FLOAT,
    end_station_longitude     FLOAT,
    bikeid                    INTEGER,
    membership_type           STRING,
    usertype                  STRING,
    birth_year                INTEGER,
    gender                    INTEGER
);



-- Create the stage with the S3 bucket
CREATE OR REPLACE STAGE citibike_trips
    URL = 's3://snowflake-workshop-lab/japan/citibike-trips';

LIST @citibike_trips;


CREATE OR REPLACE FILE FORMAT csv
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    EMPTY_FIELD_AS_NULL            = TRUE
    SKIP_HEADER                    = 1
    NULL_IF                        = ('');

-- 7b. Reset the table and its load history, then reload
TRUNCATE TABLE trips;

alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'LARGE';

COPY INTO trips
FROM @citibike_trips
FILE_FORMAT = csv
ON_ERROR    = CONTINUE
PATTERN     = '.*[.]csv.gz';

alter warehouse DATAPIPELINES_WH set WAREHOUSE_SIZE = 'XSMALL';

-- Check we got the trips information-- Check if the trips table is loaded with data
select * from trips limit 10;
