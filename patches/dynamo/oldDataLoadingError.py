# I had an issue where data was getting loaded if it was in the rising data and older than a day
# The issue is now patched but there was now bad data in dynamo that had to be cleaned.

import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os
thisLocation = os.getcwd()
sys.path.append(thisLocation+'/../../model/')  # this is for importing model utils
import utils


session = boto3.Session(profile_name='AdministratorAccess', region_name='us-east-2')
# resource vs client: https://www.learnaws.org/2021/02/24/boto3-resource-client/
dynamodb_resource = session.resource('dynamodb')  #  higher level abstractions, recommended to use, fewer methods but creating table returns a table object that you can run operations on, can also grab a Table with Table('name')
# dynamodb_client = session.client('dynamodb')  # low-level, more explicit methods. Creating table returns a dictionary

risingTable = dynamodb_resource.Table('rising')

# find posts in the rising table
datesToQuery = utils.daysUntilNow()
print("Dates to query:", datesToQuery)

postIdQueryResult = utils.queryByRangeOfDates(risingTable, datesToQuery)  # [{'postId': XXXXXX}, {'postId': YYYYYY}...]
postsOfInterest = {res['postId'] for res in postIdQueryResult}

print("Number of posts found:", len(postsOfInterest))

# get all data for these posts
# can be slow due to RCU constraints
spark = SparkSession.builder.appName('redditData').getOrCreate()
postIdData = utils.getPostIdSparkDataFrame(spark, risingTable, postsOfInterest)

# get the error data
errorData = postIdData.where(F.col('loadTSUTC').cast('long')-F.col('createdTSUTC').cast('long')>(60*60))
errorDataPd = errorData.toPandas()
print("error data count:", len(errorDataPd))

# this can take a while due to WCU constraints, consider increasing if slow
for i in range(len(errorDataPd)):
  ex = errorDataPd.iloc[i]
  res = risingTable.delete_item(
    Key={
      'postId': ex['postId'],
      'loadTSUTC': ex['loadTSUTC'].strftime('%Y-%m-%d %H:%M:%S')
    }
  )  # this just returns some information on the request
