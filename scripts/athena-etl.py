import json
import os
import boto3
import logging

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# clients / resources
athena = boto3.client('athena')
s3 = boto3.resource('s3')
glue = boto3.client('glue')

# environment variables
source_table = str(os.environ['GLUE_SOURCE_TABLE'])
glue_db = str(os.environ['GLUE_DATABASE'])
output_bucket = str(os.environ['OUTPUT_S3_BUCKET'])
encryption_option =  str(os.environ['ENCRYPTION_OPTION']) #'SSE_S3'|'SSE_KMS'|'CSE_KMS',
partitions = str(os.environ['PARTITIONS']) # ["col_name"]
sort_cols = str(os.environ['SORT_COLUMNS'])
bucket_cols = str(os.environ['BUCKET_COLUMNS'])
bucket_count = str(os.environ['BUCKET_COUNT'])
new_table = "athena_optimized_"+source_table

# ENSURE THAT OUTPUT_LOCATION DOES NOT EXIST
def empty_output_location():
    bucket = s3.Bucket(output_bucket)
    for obj in bucket.objects.filter(Prefix=new_table):
        s3.Object(bucket.name,obj.key).delete()

def remove_existing_glue_table():
    try:
        response = glue.delete_table(
            DatabaseName=glue_db,
            Name=new_table
        )
    except Exception as e:
        print("table not found!")


def build_query():
    query_string = ("CREATE TABLE "+ new_table +
       " WITH (format = 'Parquet', " +
       " parquet_compression = 'SNAPPY'," +
       " external_location = 's3://" + output_bucket + "/" + new_table + "'," +
       " partitioned_by = ARRAY"+str(partitions) + "," +
       " bucket_count = " + bucket_count + "," +
       " bucketed_by = ARRAY" + str(bucket_cols) + ")" +
       " AS SELECT * FROM " + source_table +
       " ORDER BY "+str(sort_cols)[1:-1]+ " DESC;") # does this work with Line Breaks? Prove this works. how to pass schema information? can this be done in glue instead?

    print("LOG: Query String = "+query_string)
    return query_string

def execute_query(query_string):
    try:
        response = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': glue_db
            },
            ResultConfiguration={
                'OutputLocation': "s3://"+output_bucket+"/athena/"+new_table,
                'EncryptionConfiguration': {
                    'EncryptionOption': encryption_option
                }
            }
        )
        print("LOG: SUCCESS!")
    except Exception as e:
        print("LOG: query failed to execute.")
        print(str(e))

def lambda_handler(event, context):

    print("LOG: emptying output location...")
    empty_output_location()

    print("LOG: removing glue table...")
    remove_existing_glue_table()

    print("LOG: building string...")
    query = build_query()

    print("LOG: executing query...")
    execute_query(query)