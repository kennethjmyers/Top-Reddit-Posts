import pytest
import redditUtils as ru
import praw
import tableDefinition
from collections import namedtuple
import boto3
import os


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


@pytest.fixture(scope='module')
def cfg():
  cfg_file = ru.findConfig()
  cfg = ru.parseConfig(cfg_file)
  return cfg


@pytest.fixture(scope='module')
def reddit(cfg):
  if IN_GITHUB_ACTIONS:
    pytest.skip(reason="Config not available in Github Actions.")
  return praw.Reddit(
    client_id=f"{cfg['CLIENTID']}",
    client_secret=f"{cfg['CLIENTSECRET']}",
    password=f"{cfg['PASSWORD']}",
    user_agent=f"Post Extraction (by u/{cfg['USERNAME']})",
    username=f"{cfg['USERNAME']}",
  )


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Config not available in Github Actions.")
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


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Config not available in Github Actions.")
class TestBatchWriter:
  def classSetUp(self):
    """
    If we left this at top level of the class then it won't be skipped by `skip` and `skipif`
    furthermore we can't have __init__ in a Test Class, so this is called prior to each test
    :return:
    """
    self.dynamodb_resource = boto3.resource('dynamodb')
    self.tableName = 'test'
    self.testRawTableDefinition = tableDefinition.getTableDefinition(self.tableName)
    self.testTable = ru.getOrCreateTable(self.testRawTableDefinition, self.dynamodb_resource)
    self.schema = tableDefinition.schema
    self.columns = self.schema.keys()
    self.Row = namedtuple("Row", self.columns)

  @pytest.mark.xfail(reason="BatchWriter can't accept duplicate keys")
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
    ru.batchWriter(table=testTable, data=data, schema=schema)
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
    ru.batchWriter(table=testTable, data=data, schema=schema)
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
    ru.batchWriter(table=testTable, data=data, schema=schema)
    print("diffPrimaryIndexSameSecondIndexTester test complete")


if __name__=='__main__':
  cfg_file = ru.findConfig()
  cfg = ru.parseConfig(cfg_file)

  CLIENTID = cfg['CLIENTID']
  CLIENTSECRET = cfg['CLIENTSECRET']
  PASSWORD = cfg['PASSWORD']
  USERNAME = cfg['USERNAME']

  reddit = praw.Reddit(
    client_id=f"{CLIENTID}",
    client_secret=f"{CLIENTSECRET}",
    password=f"{PASSWORD}",
    user_agent=f"Post Extraction (by u/{USERNAME})",
    username=f"{USERNAME}",
  )

  #data = test_getRedditData(reddit)
  tbw = TestBatchWriter()
  #tbw.duplicateDataTester()
  tbw.test_uniqueData()
  tbw.test_diffPrimaryIndexSameSecondIndex()
  test_deduplicateRedditData()

