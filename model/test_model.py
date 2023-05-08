import pytest
from PredictETL import Pipeline
import utils
import sqlUtils as su
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import schema
import boto3
from moto import mock_dynamodb
import sys
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, '../lambdaFunctions/getRedditDataFunction/'))
sys.path.append(os.path.join(THIS_DIR, '../'))
import configUtils as cu
import tableDefinition
import pandas as pd
import json
from decimal import Decimal
from unittest import mock


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"
os.environ['TZ'] = 'UTC'


@pytest.fixture(scope='module')
def sampleRisingData():
  d = pd.read_csv(os.path.join(THIS_DIR, 'test_data.csv'))
  # we need to change some of the values here so that they can be found by the extract method
  # particularly the load dates have to be within the last hour
  now = datetime.utcnow()
  hourAgo = now - timedelta(seconds=3600)
  d['newLoad'] = d['timeElapsedMin'].apply(lambda x: hourAgo+timedelta(seconds=60*x))
  d['loadTSUTC'] = d['newLoad'].apply(
    lambda x: f"{x.strftime('%Y-%m-%d')} {x.strftime('%H:%M:%S')}"
  )
  d['loadDateUTC'] = d['newLoad'].apply(lambda x: f"{x.strftime('%Y-%m-%d')}")
  d['loadTimeUTC'] = d['newLoad'].apply(lambda x: f"{x.strftime('%H:%M:%S')}")
  del d['newLoad']
  return d.to_json(orient='records')


# I would like to get this working but I tried for a while and it always runs into errors
# when you start trying to pass it to test functions, this may help https://stackoverflow.com/questions/49943938/unrecognizedclientexception-when-using-botocore-stubber
# @pytest.fixture
# @mock_dynamodb
# def dynamodb_resource(sampleRisingData):
#   dynamodb = boto3.resource('dynamodb')
#   # create table and write to sample data
#   tableName = 'rising'
#   td = tableDefinition.getTableDefinition(tableName=tableName)
#   table = dynamodb.create_table(**td)
#   with table.batch_writer() as batch:
#     for item in json.loads(sampleRisingData, parse_float=Decimal):  # for each row obtained
#       batch.put_item(
#         Item = item # json.loads(item, parse_float=Decimal) # helps with parsing float to Decimal
#       )
#   return dynamodb


# def test_dynamo(dynamodb_resource):
#   from boto3.dynamodb.conditions import Key, Attr
#   table = dynamodb_resource.Table('rising')
#   table.scan()
#   table.query(
#     IndexName='byLoadDate',
#     KeyConditionExpression=Key('loadDateUTC').eq('2023-05-04'),
#     FilterExpression=Attr('timeElapsedMin').gte(50),
#     ProjectionExpression='postId'
#     )


@pytest.fixture(scope='module')
def spark():
  return (
    SparkSession
      .builder
      .appName('redditData')
      .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT')
      .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT')
      .config('spark.sql.session.timeZone', 'UTC')
      .getOrCreate()
  )


@pytest.fixture(scope='module')
def model():
  testFile = os.path.join(THIS_DIR, 'pickledModels/Reddit_model_LR_20230414-061009.sav')
  return utils.loadModel(modelSaveLoc=testFile)


@pytest.fixture(scope='module')
def threshold():
  return 0.0400


@pytest.fixture(scope='module')
def modelName():
  return 'testModelName'


@pytest.fixture(scope='module')
def cfg():
  cfg_file = cu.findConfig()
  cfg = cu.parseConfig(cfg_file)
  return cfg


# need to find a way to mock this, couldn't find anything that does that
@pytest.fixture
def engine(cfg):
  try:
    e = su.makeEngine(cfg)
  except ValueError:
    e = None
  return e


# as I mentioned earlier, I'd like to change this so the dynamodb resource is a fixture but it kept throwing errors
@mock_dynamodb
def test_extract(sampleRisingData, cfg, engine, model, modelName, spark, threshold):
  dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
  # create table and write to sample data
  tableName = 'rising'
  td = tableDefinition.getTableDefinition(tableName=tableName)
  table = dynamodb.create_table(**td)
  with table.batch_writer() as batch:
    for item in json.loads(sampleRisingData, parse_float=Decimal):  # for each row obtained
      batch.put_item(
        Item=item  # json.loads(item, parse_float=Decimal) # helps with parsing float to Decimal
      )
  pipeline = Pipeline(cfg=cfg, dynamodb_resource=dynamodb, engine=engine, model=model, modelName=modelName, spark=spark, threshold=threshold)
  pipeline.extract()
  print(table.scan())
  assert pipeline.postIdData.count() == 9  # all the data that was put into the mock dynamodb


# dynamodb not needed for the below tests, only needed for extract which was tested above
@pytest.fixture
def pipeline(cfg, engine, model, modelName, spark, threshold):
  return Pipeline(cfg=cfg, dynamodb_resource=None, engine=engine, model=model, modelName=modelName, spark=spark, threshold=threshold)


@pytest.fixture(scope='module')
def aggDataDf(spark):
  # first example is a non-viral, second example should be viral
  testAggData = [{
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
    }]
  return spark.createDataFrame(testAggData, schema.aggDataSparkSchema).toPandas()


def test_createPredictions(aggDataDf, pipeline):
  pipeline.createPredictions(aggDataDf)


def test_markStepUp(aggDataDf, pipeline):
  aggData = pipeline.createPredictions(aggDataDf)
  aggData = pipeline.markStepUp(aggData)
  print(aggData.to_string())


@pytest.mark.filterwarnings("ignore:Deprecated API features detected!")
@pytest.mark.filterwarnings("ignore:The Connection.run_callable()")
def test_filterExistingData(aggDataDf, engine, pipeline):
  if engine is None:
    pytest.skip("No engine")
  aggData = pipeline.createPredictions(aggDataDf)
  aggData = pipeline.markStepUp(aggData)
  aggData = pipeline.filterExistingData(data=aggData)
  print(f"Data count after filtering existing data: {len(aggData)}")


#@pytest.mark.skip(reason="Don't want notifications sending to discord")
@mock.patch('discordUtils.discordMessageHandler')
@mock.patch('discordUtils.createMessage')
@mock.patch('discordUtils.createDM')
def test_notifyUserAboutViralPosts(mockDM, mockMessage, mockMessageHandler, aggDataDf, pipeline):
  # mocks
  mockDM.return_value = {'id':'0000000000'}
  from collections import namedtuple
  _response = namedtuple('_response', ['status_code'])
  mockMessage.return_value = _response(status_code=200)
  mockMessageHandler.return_value = None

  aggData = pipeline.createPredictions(aggDataDf)
  aggData = pipeline.markStepUp(aggData)
  viralData = aggData[aggData['stepUp'] == 1]
  pipeline.notifyUserAboutViralPosts(viralData)


@pytest.mark.filterwarnings("ignore:The Connection.run_callable()")
def test_load(aggDataDf, engine, pipeline):
  if engine is None:
    pytest.skip("No engine")
  aggData = pipeline.createPredictions(aggDataDf)
  aggData = pipeline.markStepUp(aggData)
  aggData = pipeline.filterExistingData(data=aggData)
  viralData = aggData[aggData['stepUp'] == 1]
  pipeline.load(data=viralData, tableName='scoredData')


if __name__=='__main__':
  cfg_file = cu.findConfig()
  cfg = cu.parseConfig(cfg_file)

  spark = (
    SparkSession
      .builder
      .appName('redditData')
      .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT')
      .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT')
      .config('spark.sql.session.timeZone', 'UTC')
      .config("fs.s3a.access.key", cfg['S3_access']['ACCESSKEY'])
      .config("fs.s3a.secret.key", cfg['S3_access']['SECRETKEY'])
      .getOrCreate()
  )

  # grab latest model
  model=utils.getLatestModel()

  #test_Pipeline(model, spark, threshold=0.0400)