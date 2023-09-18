--Direct Share

USE ROLE accountadmin;

CREATE SHARE share_citi;

GRANT USAGE ON DATABASE CITIBIKE TO SHARE share_citi;
GRANT USAGE ON SCHEMA CITIBIKE.PUBLIC TO SHARE share_citi;
GRANT SELECT ON TABLE CITIBIKE.PUBLIC.TRIPS TO SHARE share_citi;

SHOW GRANTS TO SHARE share_citi;

ALTER SHARE share_citi ADD ACCOUNTS=<[account to share]>;

SHOW GRANTS OF SHARE share_citi;

-- Role based share

USE ROLE accountadmin;

CREATE ROLE share_user;

GRANT USAGE ON DATABASE CITIBIKE TO ROLE share_user;
GRANT USAGE ON SCHEMA CITIBIKE.PUBLIC TO ROLE share_user;
GRANT SELECT ON TABLE CITIBIKE.PUBLIC.TRIPS TO ROLE share_user;

SHOW GRANTS TO ROLE share_user;

CREATE SHARE share_citi;
GRANT USAGE ON ROLE share_user TO SHARE share_citi;

ALTER SHARE share_citi ADD ACCOUNTS=<[account to share]>;

SHOW GRANTS OF SHARE share_citi;


