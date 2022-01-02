create or replace table Companies(
  company_name varchar,
  ceo_name varchar,
  Rank_now number,
  last_year_rank number,
  market_cpa float,
  country varchar,
  Revenue number,
  total_employees number,
  url varchar
);

select * from Companies

create or replace storage integration s3_object
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::022383934950:role/<Role_name_provided>'
  storage_allowed_locations = ('s3://snowflake_folder/snowflake/');
  

desc INTEGRATION s3_object;


create or replace file format com_file
type = csv
FIELD_DELIMITER = ','
field_optionally_enclosed_by = '"'
skip_header = 1
null_if=('Null', 'null')
empty_field_as_null = true
;

create or replace stage new_stage
url = 's3://snowflake_folder/snowflake/Companies_market_cap.csv'
file_format = com_file
STORAGE_INTEGRATION = s3_object
;


--loading data from external stage into the table
copy into Companies
from @new_stage
ON_ERROR = CONTINUE

list @new_stage

select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8 from @new_stage t;



select * from companies

