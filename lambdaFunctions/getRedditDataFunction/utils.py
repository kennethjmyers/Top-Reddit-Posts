from datetime import datetime
from configparser import ConfigParser
import os
from collections import namedtuple
from tabledefinitions import risingTableDefinition
import json
from decimal import Decimal


def findConfig() -> str:
  for f in ['./reddit.cfg', '../reddit.cfg', '../../reddit.cfg']:
    if os.path.exists(f):
      return f
  raise RuntimeError("Reddit config file not found. Place it in either ./ or ../")


def parseConfig(cfg_file: str) -> dict:
  parser = ConfigParser()
  cfg = dict()
  _ = parser.read(cfg_file)
  cfg['CLIENTID'] = parser.get("reddit_api", "CLIENTID")
  cfg['CLIENTSECRET'] = parser.get("reddit_api", "CLIENTSECRET")
  cfg['PASSWORD'] = parser.get("reddit_api", "PASSWORD")
  cfg['USERNAME'] = parser.get("reddit_api", "USERNAME")
  return cfg


def getRising(reddit, subreddit, topN=25, risingSchema=risingTableDefinition.risingSchema, verbose=False):
  topN = reddit.subreddit(subreddit).rising(limit=topN)  # rising caps out at 25
  now = datetime.utcnow().replace(tzinfo=None)
  columns = risingSchema.keys()
  Row = namedtuple("Row", columns)
  dataCollected = []
  for submission in topN:
    createdUTC = datetime.utcfromtimestamp(submission.created_utc)
    timeElapsedMin = (now - createdUTC).seconds // 60
    if timeElapsedMin >= 120:
      continue
    postId = submission.id
    title = submission.title
    score = submission.score
    numComments = submission.num_comments
    upvoteRatio = submission.upvote_ratio
    gildings = submission.gildings
    numGildings = sum(gildings.values())
    row = Row(
      postId=postId, subreddit=subreddit, title=title, createdUTC=str(createdUTC),
      timeElapsedMin=timeElapsedMin, score=score, numComments=numComments,
      upvoteRatio=upvoteRatio, numGildings=numGildings, loadTimeUTC=str(now), loadDateUTC=str(now.date()))
    dataCollected.append(row)
    if verbose:
      print(row)
      print()
  return dataCollected


def getOrCreateTable(tableDefinition, dynamodb_resource):
    existingTables = [a.name for a in dynamodb_resource.tables.all()]  # client method: dynamodb_client.list_tables()['TableNames']
    tableName = tableDefinition['TableName']
    if tableName not in existingTables:
      print(f"Table {tableName} not found, creating table")
      # create table
      # boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/service-resource/create_table.html#DynamoDB.ServiceResource.create_table
      # dynamodb keyschemas and secondary indexes: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
      table = dynamodb_resource.create_table(**tableDefinition)

      # Wait until the table exists.
      table.wait_until_exists()

    else:
      print(f"Table {tableName} exists, grabbing table...")
      table = dynamodb_resource.Table(tableName)

    # Print out some data about the table.
    print(f"Item count in table: {table.item_count}")  # this only updates every 6 hours
    return table


def batchWriter(table, data, schema):
  """
  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html#batch-writing
  I didn't bother with dealing with duplicates because shouldn't be a problem with this type of data
  no built in way to get reponses with batch_writer https://peppydays.medium.com/getting-response-of-aws-dynamodb-batchwriter-request-2aa3f81019fa

  :param table:
  :param data:
  :param schema:
  :return: None
  """
  columns = schema.keys()
  with table.batch_writer() as batch:
    for i in range(len(data)):  # for each row obtained
      batch.put_item(
        Item = json.loads(json.dumps({k:getattr(data[i], k) for k in columns}), parse_float=Decimal) # helps with parsing float to Decimal
      )