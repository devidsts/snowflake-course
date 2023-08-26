-- Note, you must use SnowSQL for this, thus open SnowSQL in the terminal
snowsql -a <account> -u <user>

-- Download sales.json to local environment (remember location)

-- Create database and home_sales table
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE SNOWTEST;
CREATE OR REPLACE SCHEMA SNOWTEST.PUBLIC;

CREATE OR REPLACE TEMPORARY TABLE SNOWTEST.PUBLIC.home_sales (
  city STRING,
  zip STRING,
  state STRING,
  type STRING DEFAULT 'Residential',
  sale_date timestamp_ntz,
  price STRING
  );

-- Create file format to hold the JSON file
CREATE OR REPLACE FILE FORMAT sf_tut_json_format
  TYPE = JSON;

-- Create stage for ingesting external file
CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage
 FILE_FORMAT = sf_tut_json_format;

-- Use PUT command to place the local file into Snowflake
PUT 'file://G:/Shared drives/IDSTS Shared Drive/Innovation In Software/Citi Training/experiments/data-load-internal/sales.json' @sf_tut_stage AUTO_COMPRESS=TRUE;
-- PUT file://C:\<file_path>\sales.json @sf_tut_stage AUTO_COMPRESS=TRUE; -- Windows
-- PUT 'file://C:/<file_path>/sales.json' @sf_tut_stage AUTO_COMPRESS=TRUE; -- Windows
-- PUT file://C:/<file_path>/sales.json @sf_tut_stage AUTO_COMPRESS=TRUE; -- Linux/MacOD

-- Copy file from stage into the database table
COPY INTO home_sales(city, state, zip, sale_date, price)
   FROM (SELECT SUBSTR($1:location.state_city,4),
                SUBSTR($1:location.state_city,1,2),
                $1:location.zip,
                to_timestamp_ntz($1:sale_date),
                $1:price
         FROM @sf_tut_stage/sales.json.gz t)
   ON_ERROR = 'continue';

-- View the file
SELECT * FROM SNOWTEST.PUBLIC.home_sales;

-- You can also ingest the JSON file without defining a table

-- Create new table but use the variant column
DROP TABLE home_sales;
CREATE OR REPLACE TABLE home_sales (
  json_column variant
  );

-- Ingest the data as before, but into the variant column
CREATE OR REPLACE FILE FORMAT sf_tut_json_format
  TYPE = JSON;

CREATE OR REPLACE TEMPORARY STAGE sf_tut_stage
 FILE_FORMAT = sf_tut_json_format;

PUT 'file://G:/Shared drives/IDSTS Shared Drive/Innovation In Software/Citi Training/experiments/data-load-internal/sales.json' @sf_tut_stage AUTO_COMPRESS=TRUE;
PUT file://C:/Users/kwame/Downloads/sales.json @sf_tut_stage AUTO_COMPRESS=TRUE;

COPY INTO home_sales(json_column)
   FROM (SELECT *
         FROM @sf_tut_stage/sales.json.gz t)
   ON_ERROR = 'continue';

-- When you select the data, you'll see everything stored as a JSON
SELECT * FROM home_sales;

-- You can extract data from the JSON and create columns as well
SELECT SUBSTR($1:location.state_city,4),
                SUBSTR($1:location.state_city,1,2),
                $1:location.zip,
                to_timestamp_ntz($1:sale_date),
                $1:price
         FROM SNOWTEST.PUBLIC.home_sales t

-- Clear resources
DROP DATABASE SNOWTEST;

-- Test Your Skills

-- Using the students.json, ingest the file into Snowflake. Use both approaches above.