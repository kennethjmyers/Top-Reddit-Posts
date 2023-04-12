import os
from configparser import ConfigParser
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from schema import fromDynamoConversion, toSparkSchema
from functools import reduce
from boto3.dynamodb.conditions import  Key
import pyspark.sql.functions as F


def findConfig() -> str:
  for f in ['./reddit.cfg', '../reddit.cfg', '../../reddit.cfg']:
    if os.path.exists(f):
      return f
  raise RuntimeError("Reddit config file not found. Place it in either ./ or ../")


def parseConfig(cfg_file: str) -> dict:
  parser = ConfigParser()
  cfg = dict()
  _ = parser.read(cfg_file)
  cfg['ACCESSKEY'] = parser.get("S3_access", "ACCESSKEY")
  cfg['SECRETKEY'] = parser.get("S3_access", "SECRETKEY")
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


def getPostIdData(table, postId):
  return table.query(
    KeyConditionExpression=Key('postId').eq(postId),
  )['Items']


def getPostIdSparkDataFrame(spark, table, postIds: list, flatten: bool = True):
  dataFrames = []
  for postId in postIds:
    res = getPostIdData(table, postId)
    res = [applyDynamoConversions(item) for item in res]
    dataFrames.append(spark.createDataFrame(res, toSparkSchema))  # convert to DF
  if flatten:
    return reduce(DataFrame.union, dataFrames)
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
  )


def getTarget(postId:str, uniqueHotPostIds:set):
  if postId in uniqueHotPostIds:
    return 1
  else:
    return 0
