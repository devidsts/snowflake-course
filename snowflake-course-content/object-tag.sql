-- Create sample databases

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE SNOWTEST; -- Database storing the tags
CREATE OR REPLACE DATABASE SNOWTEST2;
CREATE OR REPLACE DATABASE SNOWTEST3;

-- We need to create a schema for the object tags

CREATE OR REPLACE SCHEMA SNOWTEST.TAGS;

-- Let's create a tag for the different database objects

CREATE OR REPLACE TAG SNOWTEST.TAGS.tag_options allowed_values 'sales','finance';

-- We can look at the tags
SELECT GET_DDL('tag', 'tag_options');

-- We can Tag objects by using the ALTER command and SET TAG

ALTER DATABASE SNOWTEST2 SET TAG tag_options = 'sales';

-- We can ONLY add tags where there is an available option

ALTER DATABASE SNOWTEST3 SET TAG tag_options = 'IT'; -- Should display an error

-- ALTER the Tag to add the IT Option

ALTER TAG SNOWTEST.TAGS.tag_options add allowed_values 'IT';
ALTER DATABASE SNOWTEST3 SET TAG SNOWTEST.TAGS.tag_options = 'IT';

-- We can always see all the available options for a tag in Snowflake

SELECT SYSTEM$GET_TAG_ALLOWED_VALUES('SNOWTEST.TAGS.TAG_OPTIONS');

-- Furthermore, we can see what objects have various tags

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS;
SELECT SYSTEM$GET_TAG('SNOWTEST.TAGS.TAG_OPTIONS','SNOWTEST3','DATABASE')

--Clear resources

DROP DATABASE SNOWTEST;
DROP DATABASE SNOWTEST2;
DROP DATABASE SNOWTEST3;

-- Test Your Skills

-- Create a database: SNOWTEST
-- Create a schema in SNOWTEST called "PUBLIC"
-- Create a tables called animal with the columns dog, cat, apple, rabbit and food

-- Create a tagging policy to classify the columns as either an animal or food item