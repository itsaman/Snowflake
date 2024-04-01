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


CREATE STORAGE INTEGRATION aws_sf_data
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::637423509939:role/Snowflake_Integration'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-landing2024');
    
-- fetch 5 and 7 row no.
DESC INTEGRATION aws_sf_data;


-------- Copy data to table ----------------
use role sysadmin;















