create or replace table employee(
  id number,
  first_name string,
  last_name string,
  email string,
  location string,
  department string
);

create or replace file format emp_file
type = csv
FIELD_DELIMITER = ','
field_optionally_enclosed_by = '"'
skip_header = 1
empty_field_as_null = true
;


create or replace stage emp_stage
url = 's3://snowflake2512/snowflake/'
file_format  = emp_file
storage_integration = s3_object

list@emp_stage
 

--pipe

create or replace pipe emp_pipe
auto_ingest = true
as
copy into employee from @emp_stage


desc pipe emp_pipe

select * from employee

alter pipe emp_pipe refresh;

select system$pipe_status('emp_pipe')



// Snowpipe error message
SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'emp_pipe',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))

// COPY command history from table to see error message

SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
   table_name  =>  'EMPLOYEE',
   START_TIME =>DATEADD(HOUR,-2,CURRENT_TIMESTAMP())))


//to pause a pipe

alter pipe emp_pipe set pipe_execution_paused = true
