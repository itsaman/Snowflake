CREATE OR REPLACE TRANSIENT TABLE BOOK_JSON_DATA
(
OID VARCHAR,
AUTHOR VARCHAR,
TITLE VARCHAR,
BOOKTITLE VARCHAR,
YEAR NUMBER,
TYPE VARCHAR
)


-- copying date first in internal stage and then loading in the target table --

create or replace file format json_type
type = 'json'


create or replace storage integration s3_object
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam:XXXx'
  storage_allowed_locations = ('s3://snowflake/xxxx');
  
  
desc integration s3_object
  
create or replace stage internal_stg
url = 's3://snowflake/xxxx'
file_format = json_type
storage_integration  = s3_object

copy into @%BOOK_JSON_DATA
from(
select 
$1:"_id":"$oid" as OID,
$1:"author"::string as AUTHOR , 
$1:"title"::string as TITLE,
$1:"booktitle"::string as BOOKTITLE ,
$1:"year"::string as YEAR,
$1:"type"::string as TYPE 
from @internal_stg/dblp.json
(file_format =>json_type)
)


select * from @%BOOK_JSON_DATA

copy into BOOK_JSON_DATA
from @%BOOK_JSON_DATA
file_format =(type='csv')
ON_ERROR='CONTINUE'
PURGE = TRUE


select * from BOOK_JSON_DATA



---------------

--fetching the data

select v:gender from json_demo   --male

--if you need to do casting

select v:fullName::String as full_name , v:gender::String as gender from json_demo

--
select v:fullName::String as full_name,
       v:gender::String as gender,
       v:age::int as age
from json_demo
        
--Now fetching the inner data 

select v:phoneNumber.areaCode::int,
       v:phoneNumber.subscriberNumber::int
from json_demo


--fetching array in json

select v:children[0].age::int,v:children[0].gender::string, v:children[0].name::string from json_demo

--to fetch the array values we can use FLATTEN function

select 
       v:fullName::String as father_name,
       f.value:name::string as children_name, 
       f.value:age::int as children_age,
       f.value:gender::string as gender
from json_demo, table(flatten(v:children)) f


select * from json_demo, table(flatten(v:children))


--to count size of an array

select array_size(v:children) from json_demo

