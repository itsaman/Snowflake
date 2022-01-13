--Streams
--example from the official doc

create or replace table members (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);

create or replace stream member_check on table members;

create or replace table signup (
  id number(8),
  dt date
  );
  

select * from members

select * from member_check


insert into members (id,name,fee)
values
(1,'Joe',0),
(2,'Jane',0),
(3,'George',0),
(4,'Betty',0),
(5,'Sally',0);

--applying 90 dolllars 

update members 
set fee = 90 
where id in (select id from signup s where datediff(day,'2018-08-15'::date, s.dt::date)<-30)

select * from members

create or replace table members_prod (
  id number(8) not null,
  name varchar(255) default null,
  fee number(3) null
);

insert into members_prod(id, name, fee) select id,name, fee from member_check


select * from members_prod

select * from member_check

create database demo_clone clone demo_db

select system$stream_get_table_timestamp('member_check')


---Data Sharing

create database sales

create table customer 
as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."CUSTOMER"

select * from customer limit 100

--create a share 
create share sales_share;

--granted all the usages 
grant usage on database sales to share sales_share;
grant usage on schema sales.public to share sales_share;
grant select on table customer to share sales_share;

-- if we try ti execute all permission on the share
grant all on table customer to share sales_share

show grants on share sales_share;

show grants to share sales_share;


--to add an account to a share 
alter share sales_share add accounts = dz29250

desc share sales_share


