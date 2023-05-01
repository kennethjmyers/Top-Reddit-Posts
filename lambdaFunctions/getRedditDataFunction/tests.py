import redditUtils as ru
import praw
import tableDefinition
from collections import namedtuple
import boto3


def test_getRedditData(reddit):
  subreddit = "pics"
  data = ru.getRedditData(
    reddit,
    subreddit,
    topN=25,
    view='rising',
    schema=tableDefinition.schema,
    time_filter=None,
    verbose=True)
  return data


def test_deduplicateRedditData():
  schema = tableDefinition.schema
  columns = schema.keys()
  Row = namedtuple("Row", columns)
  # these are identical examples except one has a later loadTSUTC
  data = [
    Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:03:44', postId='133fkqz',
        subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
        timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0),
    Row(loadDateUTC='2023-04-30', loadTimeUTC='05:03:44', loadTSUTC='2023-04-30 05:06:44', postId='133fkqz',
        subreddit='pics', title='Magnolia tree blooming in my friends yard', createdTSUTC='2023-04-30 04:19:43',
        timeElapsedMin=44, score=3, numComments=0, upvoteRatio=1.0, numGildings=0)
  ]
  newData = ru.deduplicateRedditData(data)
  assert len(newData) == 1
  print("test_deduplicateRedditData complete")


class test_batchWriter:
  def __init__(self, dynamodb_resource):
    self.tableName = 'test'
    self.testRawTableDefinition = tableDefinition.getTableDefinition(self.tableName)
    self.testTable = ru.getOrCreateTable(self.testRawTableDefinition, dynamodb_resource)
    self.schema = tableDefinition.schema
    columns = self.schema.keys()
    self.Row = namedtuple("Row", columns)


  def duplicateDataTester(self):
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

  def uniqueDataTester(self):
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

  def diffPrimaryIndexSameSecondIndexTester(self):
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
  dynamodb_resource = boto3.resource('dynamodb')

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
  tbw = test_batchWriter(dynamodb_resource)
  #tbw.duplicateDataTester()
  tbw.uniqueDataTester()
  tbw.diffPrimaryIndexSameSecondIndexTester()
  test_deduplicateRedditData()

