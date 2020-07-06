import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta

# Get Input Params
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_bucket'
                           ])

s3_bucket = str(args['s3_bucket'])

glueContext = GlueContext(SparkContext.getOrCreate())

# Create Glue Dynamic Frame
comments_dynaFrame = glueContext.create_dynamic_frame.from_catalog(database = "reddit_db", table_name = "raw_comments")

# convert to spark data frame
comments_dataFrame = comments_dynaFrame.toDF()

# get yesterday's date
dt = datetime.now() - timedelta(days=1)

# filter spark data frame content by date
comments_dataFrame_filtered = comments_dataFrame.filter((comments_dataFrame.comment_year == dt.year) & (comments_dataFrame.comment_month == dt.month) & (comments_dataFrame.comment_day == dt.day))

# sort spark Data Frame
sorts = ["sentiment_score", "subjectivity_score", "reading_ease_score", "total_words"]
sorted_comments_dataFrame = comments_dataFrame_filtered.orderBy(*sorts, ascending=False)

# repartition filtered spark data frame
partitions = ["comment_year","comment_month", "comment_day"]
repartitioned_comments_dataFrame = sorted_comments_dataFrame.repartition(*partitions)

# switch to dynamic frame
repartitioned_comments_dynaFrame = DynamicFrame.fromDF(repartitioned_comments_dataFrame, glueContext, "repartitioned_comments_dynaFrame")

# bucketing options
buckets = ["author"]
bucket_count = 3

# send to s3
glueContext.write_dynamic_frame.from_options(frame = repartitioned_comments_dynaFrame, connection_type = "s3", connection_options = {"path":"s3://"+s3_bucket+"/glue_optimized/", "partitionKeys": partitions,  "bucketColumns": buckets, "numberOfBuckets":bucket_count}, format = "parquet")