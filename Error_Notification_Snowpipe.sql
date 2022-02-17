create or replace TABLE EMP (
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	STREETADDRESS VARCHAR(16777216),
	CITY VARCHAR(16777216),
	START_DATE DATE
);

create or replace file format emp_file
  type = csv
  field_delimiter = ','
  null_if = ('NULL', 'null')
  empty_field_as_null = true;
  
create or replace stage emp_stage
   url = 's3://pipebuck/employee/'
   credentials=(aws_key_id='@@@@@@' aws_secret_key='@@@@')
   file_format  = emp_file;

 list @emp_stage;
 
 CREATE NOTIFICATION INTEGRATION snowpipe_sns_notify
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:@@@:Snowpipe_notify'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::@@@:role/snowpipesns'
  ;
  
 desc notification integration snowpipe_sns_notify;
 
create or replace pipe emp_pipe
auto_ingest = true
error_integration = snowpipe_sns_notify
as 
    copy into "CUSTOMER_DB"."DEMO"."EMP" from @emp_stage

alter pipe emp_pipe refresh

 
select * from emp 
 
