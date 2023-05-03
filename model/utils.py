import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from schema import fromDynamoConversion, toSparkSchema
from functools import reduce
import boto3
from boto3.dynamodb.conditions import  Key, Attr
import pyspark.sql.functions as F
import pandas as pd
import pickle


def findConfig() -> str:
  """
  Finds config file locally

  :return: string of config file location
  """
  for f in ['./reddit.cfg', '../reddit.cfg', '../../reddit.cfg']:
    if os.path.exists(f):
      return f
  raise RuntimeError("Reddit config file not found. Place it in either ./ or ../")


def parseConfig(cfgFile: str) -> dict:
  """
  Read in the config data from a location to a dictionary and return that dictionary.

  :param cfgFile: location of config file. Can be an S3 location
  :return: config dictionary
  """
  parser = ConfigParser()
  cfg = dict()
  keysToRead = {
    'S3_access': ['ACCESSKEY', 'SECRETKEY', ],
    'Discord': ['BOTTOKEN', 'MYSNOWFLAKEID', 'CHANNELSNOWFLAKEID'],
    'Postgres': ['USERNAME', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
  }
  if cfgFile[:2].lower()== 's3':
    s3 = boto3.client('s3')
    pathSplit = cfgFile.replace('s3://', '').split('/')
    bucket = pathSplit[0]
    objLoc = '/'.join(pathSplit[1:])
    obj = s3.get_object(Bucket=bucket, Key=objLoc)
    _ = parser.read_string(obj['Body'].read().decode())
  else:
    _ = parser.read(cfgFile)
  for k,vList in keysToRead.items():
    for v in vList:
      cfg[v] = parser.get(k, v)
  return cfg


def dateToStr(date):
  return date.strftime('%Y-%m-%d')


def daysUntilNow(startingDate = datetime.strptime('2023-04-09', '%Y-%m-%d').date()):
  now = datetime.utcnow().date()
  dates = [dateToStr(startingDate)]
  thisDate = startingDate
  while thisDate < now:
    thisDate+=timedelta(days=1)
    dates.append(dateToStr(thisDate))
  return dates


# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/query.html
def queryByDate(table, date: str, projectionExpression: str = 'postId'):
  return table.query(
    IndexName='byLoadDate',
    KeyConditionExpression=Key('loadDateUTC').eq(date),
    ProjectionExpression=projectionExpression
  )['Items']


def flattenItems(listOfListOfItems: list) -> list:
  return [item for sublist in listOfListOfItems for item in sublist]


def queryByRangeOfDates(table, dates: list, projectionExpression: str = 'postId') -> list:
  returnedData = []
  for d in dates:
    returnedData.append(queryByDate(table, d, projectionExpression))
  return flattenItems(returnedData)


def applyDynamoConversions(dynamoRes: dict, conversionFunctions: dict = fromDynamoConversion):
  return {k:fromDynamoConversion[k](v) for k,v in dynamoRes.items()}


def getPostIdData(table, postId, **kwargs):
  return table.query(**kwargs)['Items']


def getPostIdSparkDataFrame(spark, table, postIds: set, chunkSize=10, flatten: bool = True, **kwargs):
  """
  Read from dynamo table the data for each postId in postIds.
  Optional flattening of data to single DataFrame before return

  There might be a more efficient way to stream dynamo data to spark, but this got the job done

  :param spark: sparksession
  :param table: dynamodb table to query from
  :param postIds: set of postids to query
  :param chunkSize: number of postIds to query before converting data to a spark DataFrame
  :param flatten: option to flatten data before return
  :return: list[DataFrame]|DataFrame
  """
  dataFrames = []
  chunkRes = []
  for i, postId in enumerate(postIds):
    res = getPostIdData(table, postId, KeyConditionExpression=Key('postId').eq(postId), **kwargs)
    res = [applyDynamoConversions(item) for item in res]
    chunkRes.extend(res)
    if (i+1)%chunkSize==0:  # make a new dataframe if reached chunkSize
      dataFrames.append(spark.createDataFrame(chunkRes, toSparkSchema))
      chunkRes = []  # reset chunk collection
  if len(chunkRes)>0:  # handle anything remaining
    dataFrames.append(spark.createDataFrame(chunkRes, toSparkSchema))
  if flatten:
    return reduce(DataFrame.union, dataFrames)
  else:
    return dataFrames


def getPostIdPdDataFrame(table, postIds: set, chunkSize=10, flatten: bool = True, **kwargs):
  """
  Similar to getPostIdSparkDataFrame but sometimes data is small enough you don't need spark
  Read from dynamo table the data for each postId in postIds.
  Optional flattening of data to single DataFrame before return

  There might be a more efficient way to stream dynamo data to spark, but this got the job done

  :param table: dynamodb table to query from
  :param postIds: set of postids to query
  :param chunkSize: number of postIds to query before converting data to a pandas DataFrame
  :param flatten: option to flatten data before return
  :return: list[DataFrame]|DataFrame
  """
  dataFrames = []
  chunkRes = []
  for i, postId in enumerate(postIds):
    res = getPostIdData(table, postId, KeyConditionExpression=Key('postId').eq(postId), **kwargs)
    res = [applyDynamoConversions(item) for item in res]
    chunkRes.extend(res)
    if (i+1)%chunkSize==0:  # make a new dataframe if reached chunkSize
      dataFrames.append(pd.DataFrame(chunkRes))
      chunkRes = []  # reset chunk collection
  if len(chunkRes)>0:  # handle anything remaining
    dataFrames.append(pd.DataFrame(chunkRes))
  if flatten:
    return pd.concat(dataFrames, axis=0)
  else:
    return dataFrames


def applyDataTransformations(postIdData):
  return (
    postIdData
      .groupBy('postId', 'subreddit', 'title', 'createdTSUTC')
      .agg(
      F.max(F.when(F.col('timeElapsedMin') <= 20, F.col('score'))).alias('maxScore20m')
      , F.max(F.when(F.col('timeElapsedMin').between(21, 40), F.col('score'))).alias('maxScore21_40m')
      , F.max(F.when(F.col('timeElapsedMin').between(41, 60), F.col('score'))).alias('maxScore41_60m')
      , F.max(F.when(F.col('timeElapsedMin') <= 20, F.col('numComments'))).alias('maxNumComments20m')
      , F.max(F.when(F.col('timeElapsedMin').between(21, 40), F.col('numComments'))).alias('maxNumComments21_40m')
      , F.max(F.when(F.col('timeElapsedMin').between(41, 60), F.col('numComments'))).alias('maxNumComments41_60m')
      , F.max(F.when(F.col('timeElapsedMin') <= 20, F.col('upvoteRatio'))).alias('maxUpvoteRatio20m')
      , F.max(F.when(F.col('timeElapsedMin').between(21, 40), F.col('upvoteRatio'))).alias('maxUpvoteRatio21_40m')
      , F.max(F.when(F.col('timeElapsedMin').between(41, 60), F.col('upvoteRatio'))).alias('maxUpvoteRatio41_60m')
      , F.max(F.when(F.col('timeElapsedMin') <= 20, F.col('numGildings'))).alias('maxNumGildings20m')
      , F.max(F.when(F.col('timeElapsedMin').between(21, 40), F.col('numGildings'))).alias('maxNumGildings21_40m')
      , F.max(F.when(F.col('timeElapsedMin').between(41, 60), F.col('numGildings'))).alias('maxNumGildings41_60m')
    )
    .withColumn("maxScoreGrowth21_40m41_60m",
                (F.col('maxScore41_60m') - F.col('maxScore21_40m')) / F.col('maxScore21_40m'))
    .withColumn("maxNumCommentsGrowth21_40m41_60m",
                (F.col('maxNumComments41_60m') - F.col('maxNumComments21_40m')) / F.col('maxNumComments21_40m'))
  )


def getTarget(postId:str, uniqueHotPostIds:set):
  if postId in uniqueHotPostIds:
    return 1
  else:
    return 0


def getLatestModel():
  s3 = boto3.resource('s3', region_name='us-east-2')
  bucketName = 'data-kennethmyers'
  bucket = s3.Bucket(bucketName)
  objs = bucket.objects.filter(Prefix='models/Reddit_LR_model_')
  latestModelLoc = sorted([obj.key for obj in objs])[-1]
  print(f"Latest model location: s3a://{bucketName}/{latestModelLoc}")
  modelSaveLoc = './pickledModels/latestModel.sav'
  s3_client = boto3.client('s3', region_name='us-east-2')
  s3_client.download_file('data-kennethmyers', latestModelLoc, modelSaveLoc)
  model = pickle.load(open(modelSaveLoc, 'rb'))
  return model, latestModelLoc
