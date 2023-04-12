# Contains schemas needed for reading data into spark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, FloatType
from datetime import datetime


# functions to convert data on read
fromDynamoConversion = {
  'loadDateUTC': lambda x: datetime.strptime(x, '%Y-%m-%d'),
  'loadTimeUTC': lambda x: datetime.strptime(x, '%H:%M:%S'),
  'loadTSUTC': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
  'postId': str,
  'subreddit': str,
  'title': str,
  'createdTSUTC': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
  'timeElapsedMin': int,
  'score': int,
  'numComments': int,
  'upvoteRatio': float,
  'numGildings': int
}

# https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/
toSparkSchema = StructType([
  StructField("loadDateUTC",TimestampType(),False),
  StructField("loadTimeUTC",TimestampType(),False),
  StructField("loadTSUTC",TimestampType(),False),
  StructField("postId", StringType(), False),
  StructField("subreddit", StringType(), False),
  StructField("title", StringType(), False),
  StructField("createdTSUTC", TimestampType(), False),
  StructField("timeElapsedMin", IntegerType(), True),
  StructField("score", IntegerType(), True),
  StructField("numComments", IntegerType(), True),
  StructField("upvoteRatio", FloatType(), True),
  StructField("numGildings", IntegerType(), True),
])