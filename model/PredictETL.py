#!/usr/bin/env python
import utils
import discordUtils as du
import boto3
import os
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import  Key, Attr
from pyspark.sql import SparkSession
import pandas as pd
import sqlUtils as su
import sys
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, '../'))
import configUtils as cu


os.environ['TZ'] = 'UTC'


class Pipeline:
  def __init__(self, cfg, dynamodb_resource, engine, model, modelName, spark: SparkSession, threshold=0):
    self.cfg = cfg
    self.dynamodb_resource = dynamodb_resource
    self.engine = engine
    self.model = model
    self.modelName = modelName
    self.spark = spark
    self.threshold = threshold  # if 0, step up everything

    # initializations - passed between functions
    self.postIdData = None

  def extract(self):
    """
    This job is going to run once every 10 minutes.
    First grab the date from 10 minutes ago (so if we run this at 00:00:00, this will grab data from the previous day)
    Query dynamo from this date and filtered to the last 10 minutes
    """
    ###################
    # Get Rising Data #
    ###################
    print("Gathering Rising Data...")
    now = datetime.utcnow()
    fifteenMinAgo = now - timedelta(seconds=900)
    fifteenMinAgoDate = fifteenMinAgo.strftime('%Y-%m-%d')
    fifteenMinAgoTime = fifteenMinAgo.strftime('%H:%M:%S')

    risingTable = self.dynamodb_resource.Table('rising')

    postIdQueryResult = risingTable.query(
      IndexName='byLoadDate',
      KeyConditionExpression=Key('loadDateUTC').eq(fifteenMinAgoDate) & Key('loadTimeUTC').gte(fifteenMinAgoTime),
      FilterExpression=Attr('timeElapsedMin').gte(45),
      ProjectionExpression='postId'
    )
    postIdQueryItems = postIdQueryResult['Items']
    postsOfInterest = {res['postId'] for res in postIdQueryItems}

    print("Number of posts found:", len(postsOfInterest))

    self.postIdData = utils.getPostIdSparkDataFrame(self.spark, risingTable, postsOfInterest, chunkSize=100)

    pandasTestDf = self.postIdData.limit(5).toPandas()
    print(pandasTestDf.to_string())
    print("Finished gathering Rising Data.")

  def transform(self, filterExistingData=True):
    ##################################
    # Apply all data transformations #
    ##################################
    # if you don't initialize this, you get an error when you try to broadcast the UDF
    postIdData = self.postIdData
    print("Applying transformations to Rising Data...")
    aggData = utils.applyDataTransformations(postIdData)
    aggData = aggData.toPandas().fillna(0)

    # add model name
    aggData['modelName'] = [self.modelName for _ in range(len(aggData))]

    # Generate Predictions on Data
    aggData = self.createPredictions(aggData)
    aggData = pipeline.markStepUp(aggData)
    print(aggData.to_string())

    # filter out data we've seen but the decision hasn't changed
    print("filter out existing data")
    if filterExistingData and len(aggData) > 0:
      aggData = self.filterExistingData(data=aggData)
      print(f"Data count after filtering existing data: {len(aggData)}")

    # subset to viral data
    viralData = aggData[aggData['stepUp'] == 1]

    # notify the user about this data
    pipeline.notifyUserAboutViralPosts(viralData)

    return aggData

  ############################
  # Write Data to postgresql #
  ############################
  def load(self, data, tableName):
    """
    Load aggregated data to sql table.

    :param data: aggregated data to load into sql table
    :param tableName: string containing table name to write data to
    :return: None
    """
    if len(data) < 1:
      print("No data to write to postgres")
      return
    print("Writing to postgres")
    engine = self.engine
    data = data.set_index(['postId'])
    with engine.connect() as conn:
      result = su.upsert_df(df=data, table_name=tableName, engine=conn)
    print("Finished writing to postgres")
    return

  def createPredictions(self, aggData):
    """
    Applies model to data and creates a new column for probability predicted
    """
    modelFeatures = self.model.feature_names_in_
    predictions = self.model.predict_proba(aggData[modelFeatures])[:, 1]
    aggData['predict_proba_1'] = predictions
    return aggData

  def markStepUp(self, aggData):
    """
    Step-up means we will notify the user that this post is likely to be viral.
    """
    aggData['stepUp'] = aggData['predict_proba_1'].apply(lambda x: 1 if x >= self.threshold else 0)
    return aggData

  def filterExistingData(self, data):
    engine = self.engine
    postIds = list(data['postId'])
    sql = f"""select "postId", "stepUp", 1 as "matchFound" from public."scoredData" where "postId" in ('{"','".join(postIds)}')"""
    with engine.connect() as conn:
      result = pd.read_sql(sql=sql, con=conn)
    # join data together
    joinedData = pd.merge(data, result, on=['postId', 'stepUp'], how='left')
    # filter out where match found
    joinedData = joinedData[joinedData['matchFound'] != 1]
    del joinedData['matchFound']

    return joinedData

  def notifyUserAboutViralPosts(self, viralData):
    """
    Send a SNS and discord message to the user about viral posts

    :param viralData: aggregated data that has been subsetted to what is viral
    :return: The data that we notified was viral
    """
    cfg = self.cfg
    if len(viralData) < 1:
      print("No viral data. Nothing to notify.")
      return

    viralDataString = "Found potentially viral post(s):"
    for i in range(len(viralData)):
      thisPostId = viralData.iloc[i]['postId']
      thisPostScore = viralData.iloc[i]['predict_proba_1']
      viralDataString += f"\n\thttps://reddit.com/{thisPostId}\n\t\tscore={thisPostScore:.04f}"
    viralDataString += f"\nthreshold = {self.threshold:.04f}"

    # Discord - message user
    dm = du.createDM(cfg['BOTTOKEN'], cfg['MYSNOWFLAKEID'])
    du.discordMessageHandler(cfg['BOTTOKEN'], dm['id'], viralDataString)

    # Discord - message channel
    du.discordMessageHandler(cfg['BOTTOKEN'], cfg['CHANNELSNOWFLAKEID'], viralDataString)

    return


if __name__ == "__main__":
  threshold = 0.12965  # eventually will probably put this in its own config file, maybe it differs per subreddit
  # modelName = 'models/Reddit_model_GBM_20230503-235329.sav'

  # cfg_file = cu.findConfig()
  cfg_file = 's3://data-kennethmyers/reddit.cfg'
  cfg = cu.parseConfig(cfg_file)

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

  # grab latest model
  model, modelName = utils.getLatestModel()
  # model = utils.getModel(modelName)  # alternative, pass a specific model

  dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2')  # higher level abstractions, recommended to use, fewer methods but creating table returns a table object that you can run operations on, can also grab a Table with Table('name')
  engine = su.makeEngine(cfg)

  pipeline = Pipeline(cfg=cfg, dynamodb_resource=dynamodb_resource, engine=engine, model=model, modelName=modelName, spark=spark, threshold=threshold)
  pipeline.extract()
  data = pipeline.transform()
  pipeline.load(data=data, tableName='scoredData')
