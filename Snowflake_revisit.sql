use role sysadmin;

create database if not exists ecommerce_db;
create schema if not exists ecommeerce_liv;

grant all on database ecommerce_db to role sysadmin;
grant all on schema ecommerce_db.ecommeerce_liv to role sysadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce_db.ecommeerce_liv to role sysadmin;

create or replace warehouse product_wh with warehouse_size = 'X-LARGE';

use warehouse compute_wh;
USE DATABASE ecommerce_db;
USE SCHEMA ecommeerce_liv;

-- creating table

CREATE OR REPLACE TABLE LINEITME CLUSTER BY (L_SHIPDATE) AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM;
CREATE OR REPLACE TABLE ORDERS AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS limit 1000;

--Transient table does not have failsafe and timetravel

create or replace transient table trans_order as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS limit 30;

select * from trans_order;


----------VIEWS-------
-- Regular
-- Materialized
-- Secure

-- Materialized views are helpfull when we need to imporve performance of external table 

CREATE OR REPLACE VIEW Order_view as select * from ORDERS where O_ORDERPRIORITY = '1-URGENT';

Create MATERIALIZED VIEW ORDER_MATERIALIZED;

CREATE MATERIALIZED VIEW ORDER_MATERIALIZED
--REFRESH DAILY
AS
SELECT O_ORDERPRIORITY, count(1) AS orders_priority_count
FROM ORDERS
GROUP BY 1;



create or replace secure view order_secure as select * from orders limit 10;

select * from order_secure;

alter MATERIALIZED view ORDER_MATERIALIZED CLUSTER BY(O_ORDERDATE);

-- Partitions and Clustering

-- Micro-partitions help categorize data for faster searches based on specific values.
-- Clustering keys help organize data physically for efficient storage and retrieval in a specific order.

USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000;

select system$clustering_information('LINEITEM');
show tables like '%LINE%';


select * from LINEITEM limit 20000;

alter session set use_cached_result = false;

select * from lineitem where l_shipdate = '1998-12-01' limit 10000;

select SYSTEM$CLUSTERING_DEPTH('LINEITEM')


------------Integration object to s3----------------------

use role accountadmin;


CREATE or replace STORAGE INTEGRATION aws_sf_data
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::637423509939:role/Snowflake_Integration'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-landing2024');
    
-- fetch 5 and 7 row no.
DESC INTEGRATION aws_sf_data;


-------- Copy data to table ----------------
use role sysadmin;
use schema ECOMMERCE_DB.ECOMMEERCE_LIV;

--Table
CREATE OR REPLACE TABLE ARTIST(
    artist_id varchar,
    name varchar,
    artist_popularity varchar,
    artist_genres VARIANT,
    followers varchar
);


-- File Format

CREATE OR REPLACE FILE FORMAT artist_format
TYPE = CSV
COMPRESSION = AUTO
FIELD_DELIMITER = ' '
RECORD_DELIMITER = '/n'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;


-- Stage

create or replace stage artist_stage_new
storage_integration = aws_sf_data
url = 's3://snowflake-landing2024/ecommerce_dev/lineitems/lineitem_csv/Spotify artist data 2023.tsv'
file_format = artist_format;

list @artist_stage;

SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12 FROM @artist_stage_new t;


--Copy command

copy into ARTIST from @artist_stage_new
VALIDATION_MODE = RETURN_ALL_ERRORS
ON_ERROR = CONTINUE;

select * from ARTIST;


INSERT INTO ARTIST (artist_id, name, artist_popularity, followers,genre_0,genre_1,genre_2,genre_3,genre_4,genre_5,genre_6) SELECT t.$1, t.$2, t.$3, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, t.$12 FROM @artist_stage t;


-- Ingest JSON data





