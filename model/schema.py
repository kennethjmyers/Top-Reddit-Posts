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
  'subscribers': int,
  'activeUsers': int,
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
  StructField("subscribers", IntegerType(), False),
  StructField("activeUsers", IntegerType(), False),
  StructField("title", StringType(), False),
  StructField("createdTSUTC", TimestampType(), False),
  StructField("timeElapsedMin", IntegerType(), True),
  StructField("score", IntegerType(), True),
  StructField("numComments", IntegerType(), True),
  StructField("upvoteRatio", FloatType(), True),
  StructField("numGildings", IntegerType(), True),
])

aggDataSparkSchema = StructType([
  StructField("postId",StringType(),False),
  StructField("subreddit",StringType(),False),
  StructField("title",StringType(),False),
  StructField("createdTSUTC", TimestampType(), False),
  StructField("maxScore20m", IntegerType(), False),
  StructField("maxScore21_40m", IntegerType(), False),
  StructField("maxScore41_60m", IntegerType(), False),
  StructField("maxNumComments20m", IntegerType(), False),
  StructField("maxNumComments21_40m", IntegerType(), False),
  StructField("maxNumComments41_60m", IntegerType(), False),
  StructField("maxUpvoteRatio20m", FloatType(), False),
  StructField("maxUpvoteRatio21_40m", FloatType(), False),
  StructField("maxUpvoteRatio41_60m", FloatType(), False),
  StructField("maxNumGildings20m", IntegerType(), False),
  StructField("maxNumGildings21_40m", IntegerType(), False),
  StructField("maxNumGildings41_60m", IntegerType(), False),
  StructField("maxScoreGrowth21_40m41_60m", FloatType(), False),
  StructField("maxNumCommentsGrowth21_40m41_60m", FloatType(), False),
])