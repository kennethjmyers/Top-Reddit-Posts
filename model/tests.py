from PredictETL import Pipeline
import utils
import os
from datetime import datetime
from pyspark.sql import SparkSession


os.environ['TZ'] = 'UTC'


def testPipeline(model, spark:SparkSession, threshold):
  # first example is a non-viral, second example should be viral
  testAggData = [
    {
      'postId': '1234567',
      'subreddit': 'pics',
      'title': 'Test1',
      'createdTSUTC': datetime.strptime('2023-04-19 03:14:30', '%Y-%m-%d %H:%M:%S'),
      'maxScore20m': 0,
      'maxScore21_40m': 0,
      'maxScore41_60m': 21,
      'maxNumComments20m': 0,
      'maxNumComments21_40m': 0,
      'maxNumComments41_60m': 1,
      'maxUpvoteRatio20m': 0.0,
      'maxUpvoteRatio21_40m': 0.0,
      'maxUpvoteRatio41_60m': 0.9700000286102295,
      'maxNumGildings20m': 0,
      'maxNumGildings21_40m': 0,
      'maxNumGildings41_60m': 0,
      'maxScoreGrowth21_40m41_60m': 0.0,
      'maxNumCommentsGrowth21_40m41_60m': 0.0,
    },
    {
      'postId': '1234568',
      'subreddit': 'pics',
      'title': 'Test2',
      'createdTSUTC': datetime.strptime('2023-04-20 03:14:30', '%Y-%m-%d %H:%M:%S'),
      'maxScore20m': 10,
      'maxScore21_40m': 29,
      'maxScore41_60m': 67,
      'maxNumComments20m': 1,
      'maxNumComments21_40m': 6,
      'maxNumComments41_60m': 9,
      'maxUpvoteRatio20m': 0.95,
      'maxUpvoteRatio21_40m': 0.95,
      'maxUpvoteRatio41_60m': 0.94,
      'maxNumGildings20m': 0,
      'maxNumGildings21_40m': 0,
      'maxNumGildings41_60m': 0,
      'maxScoreGrowth21_40m41_60m': 1.1,
      'maxNumCommentsGrowth21_40m41_60m': 0.5,
    },
    {
      'postId': '1234569',
      'subreddit': 'pics',
      'title': 'Test3',
      'createdTSUTC': datetime.strptime('2023-04-20 03:14:30', '%Y-%m-%d %H:%M:%S'),
      'maxScore20m': 10,
      'maxScore21_40m': 29,
      'maxScore41_60m': 150,
      'maxNumComments20m': 1,
      'maxNumComments21_40m': 6,
      'maxNumComments41_60m': 20,
      'maxUpvoteRatio20m': 0.95,
      'maxUpvoteRatio21_40m': 0.95,
      'maxUpvoteRatio41_60m': 0.94,
      'maxNumGildings20m': 0,
      'maxNumGildings21_40m': 0,
      'maxNumGildings41_60m': 0,
      'maxScoreGrowth21_40m41_60m': 1.1,
      'maxNumCommentsGrowth21_40m41_60m': 0.5,
    }
  ]
  testAggDataDf = spark.createDataFrame(testAggData, schema.aggDataSparkSchema).toPandas()
  pipeline = Pipeline(cfg=cfg, model=model, spark=spark, threshold=threshold)
  # testing various methods of the pipeline using test data
  aggData = pipeline.createPredictions(testAggDataDf)
  aggData = pipeline.markStepUp(aggData)
  print(aggData.to_string())
  aggData = pipeline.filterExistingData(data=aggData)
  print(f"Data count after filtering existing data: {len(aggData)}")
  viralData = aggData[aggData['stepUp'] == 1]
  pipeline.notifyUserAboutViralPosts(viralData)
  pipeline.load(data=aggData, tableName='scoredData')
  return True


if __name__=='__main__':
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

  # grab latest model
  model=utils.getLatestModel()

  testPipeline(model, spark, threshold=0.0400)