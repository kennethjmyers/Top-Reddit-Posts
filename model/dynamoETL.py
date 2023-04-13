#!/usr/bin/env python
# coding: utf-8
import utils
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import boto3
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
import os


# Forcing Timezone keeps things consistent with running on aws and without it timestamps get additional
# timezone conversions when writing to parquet. Setting spark timezone was not enough to fix this
os.environ['TZ'] = 'UTC'

cfg_file = utils.findConfig()
cfg = utils.parseConfig(cfg_file)

spark = (
  SparkSession
  .builder
  .appName('redditData')
  .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT')
  .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT')
  .config('spark.sql.session.timeZone', 'UTC')
  .config("fs.s3a.access.key", cfg['ACCESSKEY'])
  .config("fs.s3a.secret.key", cfg['SECRETKEY'])
  .getOrCreate()
)


class Pipeline:
  def __init__(self, spark=spark):
    self.spark = spark
    session = boto3.Session(profile_name='AdministratorAccess', region_name='us-east-2')
    # resource vs client: https://www.learnaws.org/2021/02/24/boto3-resource-client/
    self.dynamodb_resource = session.resource('dynamodb')  #  higher level abstractions, recommended to use, fewer methods but creating table returns a table object that you can run operations on, can also grab a Table with Table('name')
    # dynamodb_client = session.client('dynamodb')  # low-level, more explicit methods. Creating table returns a dictionary

    # initializations - passed between functions
    self.postIdData = None
    self.uniqueHotPostIds = None

  def extract(self):
    ###################
    # Get Rising Data #
    ###################
    print("Gathering Rising Data...")
    risingTable = self.dynamodb_resource.Table('rising')

    datesToQuery = utils.daysUntilNow()
    print("Dates to query:", datesToQuery)

    postIdQueryResult = utils.queryByRangeOfDates(risingTable, datesToQuery)  # [{'postId': XXXXXX}, {'postId': YYYYYY}...]
    postsOfInterest = {res['postId'] for res in postIdQueryResult}

    print("Number of posts found:", len(postsOfInterest))

    # this can take a while due to read constraints placed on dynamo db, consider increasing RCU on database
    # it can also be slow because converts each dynamodb partition to a spark dataframe,
    # this was done so that it would scale better on a distributed system
    # over keeping all the data in python in one node and trying to then move it to spark
    self.postIdData = utils.getPostIdSparkDataFrame(self.spark, risingTable, postsOfInterest)
    pandasTestDf = self.postIdData.limit(5).toPandas()
    print(pandasTestDf.to_string())
    print("Finished gathering Rising Data.")

    ###############
    # Get Targets #
    ###############
    print("Gathering Hot Data...")
    hotTable = self.dynamodb_resource.Table('hot')

    hotPosts = utils.queryByRangeOfDates(hotTable, datesToQuery)
    self.uniqueHotPostIds = set([p['postId'] for p in hotPosts])

    # the hot posts are usually not a very long list and we really only need this for the purpose of creating targets
    print("unique hot postIds:", self.uniqueHotPostIds)
    print("Finished gathering Hot Data.")

  def transform(self):
    ##################################
    # Apply all data transformations #
    ##################################
    # if you don't initialize this, you get an error when you try to broadcast the UDF
    postIdData = self.postIdData
    uniqueHotPostIds = self.uniqueHotPostIds
    print("Applying transformations to Rising Data...")
    aggData = utils.applyDataTransformations(postIdData)

    print("Creating Targets for Rising Data from Hot Data")
    getTargetUDF = udf(lambda x: utils.getTarget(x, uniqueHotPostIds), returnType=IntegerType())

    aggData = aggData.withColumn('target', getTargetUDF(F.col('postId')))
    print("Finished gathering data targets.")

    return aggData

    # by aggregating the data, there should be an at most 60x reduction in the data (since data can be collected once every minute)
    # this can be slow because it has to go through all of the transformations and does not scale well
    # aggDataPd = aggData.toPandas()
    # print(len(aggDataPd))
    # aggDataPd.head()
    # aggDataPd[aggDataPd['target']==1]
    # for pId in aggDataPd[aggDataPd['target']==1]['postId']:
    #   print('https://reddit.com/'+pId)

    # At the time of writing, I've only collected about 1.5 days of data, with 7 viral posts (not a very large amount although that was to be expected). Interestingly, I've noticed that of the viral posts I have,
    #
    # - the one that had the most upvotes after an hour was actually the least viral,
    # - while the one with the least upvotes was actually the most viral.
    # - but that post with the least upvotes had the second most comments of the viral posts, 24 comments, so maybe it would be captured by the model
    #
    # I'm considering extending the time out to 90-120 minutes for data collection. However, the point was to get to a post early when there were relatively few comments. That most viral post had 24 comments after an hour and even that is a lot and any new replies are likely to be buried.

  ###############################
  # Write Spark DataFrame to S3 #
  ###############################
  #
  # This is basically our model data and what we will use to train a model. I used spark to write to s3 to future proof this if the data was too large to fit in pandas on driver.
  #
  # If you get an error here then you probably need to download hadoop-aws-*.jar (ex: [3.2.0](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.2.0)) and aws-java-sdk-bundle-*.jar (ex: [1.11.375](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.11.375))
  #
  # - for hadoop-aws-*.jar this should match the version of other hadoop jars in $SPARK_HOME/jars/
  # - for aws-java-sdk-bundle-*.jar you will need to check the version dependency of hadoop-aws-*.jar on the maven website. Do NOT use the upgraded version, use the version that hadoop-aws was created with.
  #
  # You may not need to add these dependencies to the configs, but you may need to restart the kernel and rerun things.
  #
  # These links may help:
  #
  # - [SO link 1](https://stackoverflow.com/questions/58415928/spark-s3-error-java-lang-classnotfoundexception-class-org-apache-hadoop-f?answertab=scoredesc#tab-top)
  # - [SO link 2](https://stackoverflow.com/questions/44411493/java-lang-noclassdeffounderror-org-apache-hadoop-fs-storagestatistics/44500698#44500698)
  # - [SO link 3](https://stackoverflow.com/questions/64547468/pyspark-s3-error-java-lang-noclassdeffounderror-com-amazonaws-amazonserviceex)
  # - [Tutorial](https://notadatascientist.com/running-apache-spark-and-s3-locally/)
  # - [Hadoop Troubleshooting](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
  def load(self, data, location):
    print("Writing to S3")
    data.write.parquet(location, mode="overwrite")
    print("Finished writing to S3")


pipeline = Pipeline()
pipeline.extract()
data = pipeline.transform()
pipeline.load(data, "s3a://data-kennethmyers/redditAggregatedData.parquet")
