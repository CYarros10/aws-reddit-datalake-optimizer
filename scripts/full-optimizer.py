import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame




# Get Input Params
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_bucket'
                           ])

s3_bucket = str(args['s3_bucket'])


glueContext = GlueContext(SparkContext.getOrCreate())

# Create Glue Dynamic Frame
comments_dynaFrame = glueContext.create_dynamic_frame.from_catalog(database = "reddit_db", table_name = "reddit_comments")


# switch to spark data frame
comments_dataFrame = comments_dynaFrame.toDF()

# Sort spark Data Frame
sorts = ["sentiment_score", "subjectivity_score", "reading_ease_score", "total_words"]
sorted_comments_dataFrame = comments_dataFrame.orderBy(*sorts, ascending=False)

# repartition spark data frame
partitions = ["comment_year","comment_month", "comment_day"]
repartitioned_comments_dataFrame = sorted_comments_dataFrame.repartition(*partitions)

# switch back to glue dynamic frame
final_comments_dynaFrame = DynamicFrame.fromDF(repartitioned_comments_dataFrame, glueContext, "final_comments_dynaFrame")

# bucketing options
buckets = ["author"]
bucket_count = 3

# Data Sink for Columnar based queries : glue_optimized_parquet
glueContext.write_dynamic_frame.from_options(frame = final_comments_dynaFrame, connection_type = "s3", connection_options = {"path":"s3://"+s3_bucket+"/glue_optimized/", "partitionKeys": partitions, "bucketColumns": buckets, "numberOfBuckets":bucket_count}, format = "parquet")


