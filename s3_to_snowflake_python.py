import pandas as pd
import snowflake.connector as sf
import boto3 
from snowflake.connector.pandas_tools import write_pandas 

#AWS Keys
service_name='s3'
region_name=''
aws_access_key_id=''
aws_secret_access_key=''

#for connecting boto to s3
s3 = boto3.resource(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

#snowflake account details

user=""
password=""
account="ap-south-1.aws";
database="RAMU"
warehouse="COMPUTE_WH"
schema="PUBLIC"
role="ACCOUNTADMIN"

#connection string
conn=sf.connect(user=user,password=password,account=account);


def run_query(conn, query):
    cursor = conn.cursor();
    cursor.execute(query);
    cursor.close();



statement_1='use warehouse '+warehouse;
statement2='alter warehouse '+warehouse+" resume";
statement3="use database "+database;
statement4="use role "+role;

run_query(conn,statement_1)
run_query(conn,statement2)
run_query(conn,statement3)
run_query(conn,statement4)



for obj in s3.Bucket('demoshow').objects.all():
    df=pd.read_csv(obj.get()['Body'])
    df.columns = ['SEPALLENGTH', 'SEPALWIDTH', 'PETALLENGTH', 'PETALWIDTH', 'CLASSNAME'];
    write_pandas(conn, df, 'VIDEO')
    print(df)
