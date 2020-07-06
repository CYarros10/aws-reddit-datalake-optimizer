# aws-datalake-optimizer

## Scheduled, Automated Data Optimization

Automatically optimize your data on a scheduled basis to ensure query performance and cost optimization of analytical services like Amazon Athena. Results in cost savings of 99.9%

### Scope, Audience

100-200 level Data Engineers interested in learning how to optimize a sample data lake.

### About

Athena is priced by the amount of data it scans. Therefore you are inclined to optimize your queries and data to lower your costs.

This is a POC that deploys a data lake / streaming architecture as a foundation, and then walks through a tutorial to learn how to optimize this data for Athena queries. Compare costs of running queries on raw data vs. optimized data.

[Athena Performance Tuning guidelines found here.](https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/)

----

## Architecture

![Stack-Resources](images/architecture-design-pattern.png)

----

## Deployment

### 1. Deploy the Reddit Streaming Datalake

1. If you have not already done so, please follow and deploy the infrastructure defined in the [Reddit Streaming Datalake repository](https://github.com/CYarros10/reddit-streaming-datalake)

2. When done, return here.

### 2. Create S3 Bucket for code

1. Go to the S3 Console and create a bucket for the code scripts that are required for this tutorial.

Upload the following scripts to your S3 bucket:

- scripts/athena-etl.zip
- scripts/daily-optimizer.py
- scripts/full-optimizer.py

### 3. Deploy the Data Lake Optimizer resources

1. Go to CloudFormation Console and create a stack using templates/cloudformation.yaml

2. Enter the parameters, using resource info (source bucket) from the datalake infrastructure created in step 1.

----

### What is created?

#### Optimizing Scripts/Jobs

1. **athena-etl** - A daily scheduled Lambda that uses boto3 athena library to optimize an entire s3 location / glue table
2. **full_optimizer** - A Glue Job that optimizes an entire s3 location / glue table
3. **daily_optimizer** - A daily scheduled Glue Job that optimizes the previous day of data in an s3 location / glue table

#### Crawlers

1. Glue Optimized Crawler that builds and manages Glue data catalog schema for **glue_job_optimized_reddit_comments** table
2. Glue Raw Crawler that builds and manages Glue data catalog schema for **reddit_comments** table

#### Schedulers

1. Glue Trigger scheduled to initiate **daily_optimizer** Glue Job once every 24 hours.
2. CloudWatch Event scheduled rule to initiate **athena-etl** lambda once every 24 hours.

### How do I decide what to use?

1. If you have small amounts of data or want to utilize a cost-effective solution, use **athena-etl** lambda
2. If you have a large amount of existing data that you want optimized, use the **full_optimizer** Glue Job
3. If you are streaming large amounts of data daily, use the **daily_optimizer** Glue Job

## Viewing Results

By default, the scheduled **athena-etl** lambda and **daily_optimizer** glue job will execute once a day. The glue crawlers will crawl the raw and optimized data on a fixed daily schedule as well. These schedules can be manually updated to your liking.

The **full_optimizer** glue job can be run manually. You'll want to run **rGlueCrawlerSource** to get updated schema information before the Glue Job begins.

### Step 4

Go to the newly created S3 bucket containing streamed data (ex. reddit-datalake).  you'll see the raw data under **reddit_comments** folder.  Once the data optimizers kick off, you'll see new folders created:

* **athena_optimized**
* **glue_optimized**

each containing the optimized data to be queried.

### Step 5

Now you can query your optimized data in [Athena](https://console.aws.amazon.com/athena).

To see how much money you save by querying optimized data vs. raw data, refer to the [Athena Pricing Guide](https://aws.amazon.com/athena/pricing/)

With the example queries below, you'll see cost savings, on average, of **99.9%**

----

### Example queries:

#### total number of comments

raw:

        select count(*)
        from reddit_comments;

optimized:

        select count(comment_id)
        from athena_optimized_reddit_comments;

----

#### total comments collected per subreddits

raw:

        select count(*) as num_comments, subreddit
        from reddit_comments
        group by subreddit
        order by num_comments DESC;

optimized:

        select count(*) as num_comments, subreddit
        from athena_optimized_reddit_comments
        group by subreddit
        order by num_comments DESC;

----

#### average sentiment per subreddits

raw:

        select round(avg(sentiment_score), 4) as avg_sentiment_score, subreddit
        from reddit_comments
        group by subreddit
        order by avg_sentiment_score DESC;

optimized:

        select round(avg(sentiment_score), 4) as avg_sentiment_score, subreddit
        from athena_optimized_reddit_comments
        group by subreddit
        order by avg_sentiment_score DESC;

----

#### list all Subreddits

raw:

        select distinct(subreddit)
        from reddit_comments

optimized:

        select distinct(subreddit)
        from athena_optimized_reddit_comments

----

#### top 10 most positive comments by subreddit

raw:

        select subreddit, comment_text
        from reddit_comments
        where subreddit = 'news'
        order by sentiment_score DESC
        limit 10;

optimized:

        select subreddit, comment_text
        from athena_optimized_reddit_comments
        where subreddit = 'news'
        order by sentiment_score DESC
        limit 10;

----

#### most active subreddits and their sentiment

raw:

        select subreddit, count(*) as num_comments, round(avg(sentiment_score), 4) as avg_sentiment_score
        from reddit_comments
        group by subreddit
        order by num_comments DESC;

optimized:

        select subreddit, count(*) as num_comments, round(avg(sentiment_score), 4) as avg_sentiment_score
        from athena_optimized_reddit_comments
        group by subreddit
        order by num_comments DESC;

----

#### search term frequency by subreddit where comments greater than 5

raw:

        select subreddit, count(*) as comment_occurrences
        from reddit_comments
        where comment_text like '%puppy%'
        group by subreddit
        having count(*) > 5
        order by comment_occurrences desc;

optimized:

        select subreddit, count(*) as comment_occurrences
        from athena_optimized_reddit_comments
        where comment_text like '%puppy%'
        group by subreddit
        having count(*) > 5
        order by comment_occurrences desc;

----

#### search term sentiment by subreddit

raw:

        select subreddit, round(avg(sentiment_score), 4) as avg_sentiment_score
        from reddit_comments
        where comment_text like '%puppy%'
        group by subreddit
        having count(*) > 5
        order by avg_sentiment_score desc;

optimized:

        select subreddit, round(avg(sentiment_score), 4) as avg_sentiment_score
        from athena_optimized_reddit_comments
        where comment_text like '%puppy%'
        group by subreddit
        having count(*) > 5
        order by avg_sentiment_score desc;

----

#### top 25 most positive comments about a search term

raw:

        select subreddit, author, comment_text, sentiment_score
        from reddit_comments
        where comment_text like '%puppy%'
        order by sentiment_score desc
        limit 25;

optimized:

        select subreddit, author, comment_text, sentiment_score
        from athena_optimized_reddit_comments
        where comment_text like '%puppy%'
        order by sentiment_score desc
        limit 25;

----

#### total sentiment for search term

raw:

        SELECT round(avg(sentiment_score), 4) as avg_sentiment_score
        FROM (
        SELECT subreddit, author, comment_text, sentiment_score
        FROM reddit_comments
        WHERE comment_text LIKE '%puppy%')

optimized:

        SELECT round(avg(sentiment_score), 4) as avg_sentiment_score
        FROM (
        SELECT subreddit, author_name, comment_text, sentiment_score
        FROM athena_optimized_reddit_comments
        WHERE comment_text LIKE '%puppy%')
