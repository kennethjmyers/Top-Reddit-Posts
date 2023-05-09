import pytest
import redditUtils as ru
import praw
import tableDefinition
from collections import namedtuple
import boto3
import sys
import os
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, '../../'))
import configUtils as cu
import pickle
from moto import mock_dynamodb


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


@pytest.fixture(scope='module')
def cfg():
  cfg_file = cu.findConfig()
  cfg = cu.parseConfig(cfg_file)
  return cfg


@pytest.fixture(scope='module')
def reddit(cfg):
  if IN_GITHUB_ACTIONS:
    return pickle.load(open(os.path.join(THIS_DIR, 'test_reddit.sav'), 'rb'))
  redditcfg = cfg['reddit_api']
  return praw.Reddit(
    client_id=f"{redditcfg['CLIENTID']}",
    client_secret=f"{redditcfg['CLIENTSECRET']}",
    password=f"{redditcfg['PASSWORD']}",
    user_agent=f"Post Extraction (by u/{redditcfg['USERNAME']})",
    username=f"{redditcfg['USERNAME']}",
  )


def test_getRedditData(reddit):
  subreddit = "pics"
  ru.getRedditData(
    reddit,
    subreddit,
    topN=25,
    view='rising',
    schema=tableDefinition.schema,
    time_filter=None,
    verbose=True)


@pytest.fixture(scope='module')
def duplicatedData():
  schema = tableDefinition.schema
  columns = schema.keys()
  Row = namedtuple("Row", columns)
  # these are identical examples except one has a later loadTSUTC
  return [
    Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
        subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
        timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0),
    Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:06:44', postId='133fkqz',
        subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
        timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0)
  ]


def test_deduplicateRedditData(duplicatedData):
  newData = ru.deduplicateRedditData(duplicatedData)
  assert len(newData) == 1
  print("test_deduplicateRedditData complete")


@mock_dynamodb
class TestBatchWriter:


  def classSetUp(self):
    """
    If we left this at top level of the class then it won't be skipped by `skip` and `skipif`
    furthermore we can't have __init__ in a Test Class, so this is called prior to each test
    :return:
    """
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    # create table and write to sample data
    tableName = 'rising'
    td = tableDefinition.getTableDefinition(tableName=tableName)
    self.testTable = dynamodb.create_table(**td)
    self.schema = tableDefinition.schema
    self.columns = self.schema.keys()
    self.Row = namedtuple("Row", self.columns)

  @pytest.mark.xfail(reason="BatchWriter fails on duplicate keys. This might xpass, possibly a fault in mock object.")
  def test_duplicateData(self):
    self.classSetUp()
    testTable = self.testTable
    schema = self.schema
    Row=self.Row

    data = [
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
         subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
         timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0),
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
          subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
          timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0)
     ]
    from redditUtils import batchWriter
    batchWriter(table=testTable, data=data, schema=schema)
    print("duplicateDataTester test complete")

  def test_uniqueData(self):
    self.classSetUp()
    testTable = self.testTable
    schema = self.schema
    Row = self.Row

    data = [
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
          subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
          timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0),
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fqj7',
          subreddit='pics', title='A piece of wood sticking up in front of a fire.', createdTSUTC='2023-04-30 04:29:23',
          timeElapsedMin=34, score=0, numComments=0, upvoteRatio=0.4, numGildings=0)
    ]
    from redditUtils import batchWriter
    batchWriter(table=testTable, data=data, schema=schema)
    print("uniqueDataTester test complete")

  def test_diffPrimaryIndexSameSecondIndex(self):
    self.classSetUp()
    testTable = self.testTable
    schema = self.schema
    Row = self.Row

    data = [
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
          subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
          timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0),
      Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqy',
          subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
          timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0)
    ]
    from redditUtils import batchWriter
    batchWriter(table=testTable, data=data, schema=schema)
    print("diffPrimaryIndexSameSecondIndexTester test complete")
