from datetime import datetime
from configparser import ConfigParser
import os
from collections import namedtuple
import tableDefinition
import json
from decimal import Decimal


def findConfig() -> str:
  # searches for main file, falls back to example file if not found
  fileList = [
    './reddit.cfg',
    '../reddit.cfg',
    '../../reddit.cfg',
    './example_reddit.cfg',
    '../example_reddit.cfg',
    '../../example_reddit.cfg'
  ]
  for f in fileList:
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


def getRedditData(reddit, subreddit, topN=25, view='rising', schema=tableDefinition.schema, time_filter=None, verbose=False):
  """
  Uses PRAW to get data from reddit using defined parameters. Returns data in a list of row based data.

  :param reddit: PRAW reddit object
  :param subreddit: subreddit name
  :param topN: Number of posts to return
  :param view: view to look at the subreddit. rising, top, hot
  :param schema: schema that describes the data. Dynamo is technically schema-less
  :param time_filter: range of time to look at the data. all, day, hour, month, week, year
  :param verbose: if True then prints more information
  :return: list[Row[schema]], Row is a namedtuple defined by the schema
  """
  assert topN <= 25  # some, like rising, cap out at 25 and this also is to limit data you're working with
  assert view in {'rising', 'top' , 'hot'}
  if view == 'top':
    assert time_filter in {"all", "day", "hour", "month", "week", "year"}
  if view == 'rising':
    topN = reddit.subreddit(subreddit).rising(limit=topN)
  elif view == 'hot':
    topN = reddit.subreddit(subreddit).hot(limit=topN)
  elif view == 'top':
    topN = reddit.subreddit(subreddit).top(time_filter=time_filter, limit=topN)

  now = datetime.utcnow().replace(tzinfo=None, microsecond=0)
  columns = schema.keys()
  Row = namedtuple("Row", columns)
  dataCollected = []
  for submission in topN:
    createdTSUTC = datetime.utcfromtimestamp(submission.created_utc)
    timeSincePost = now - createdTSUTC
    timeElapsedMin = timeSincePost.seconds // 60
    timeElapsedDays = timeSincePost.days
    if view=='rising' and (timeElapsedMin > 60 or timeElapsedDays>0):  # sometime rising has some data that's already older than an hour or day, we don't want that
      continue
    postId = submission.id
    title = submission.title
    score = submission.score
    numComments = submission.num_comments
    upvoteRatio = submission.upvote_ratio
    gildings = submission.gildings
    numGildings = sum(gildings.values())
    row = Row(
      postId=postId, subreddit=subreddit, title=title, createdTSUTC=str(createdTSUTC),
      timeElapsedMin=timeElapsedMin, score=score, numComments=numComments,
      upvoteRatio=upvoteRatio, numGildings=numGildings,
      loadTSUTC=str(now), loadDateUTC=str(now.date()), loadTimeUTC=str(now.time()))
    dataCollected.append(row)
    if verbose:
      print(row)
      print()
  return dataCollected


def deduplicateRedditData(data):
  """
  Deduplicates the reddit data. Sometimes there are duplicate keys which throws an error
  when writing to dynamo. It is unclear why this happens but I suspect it is an issue with PRAW.

  :param data: list[Row[schema]]
  :return: deduplicated data
  """
  postIds = set()
  newData = []
  # there really shouldn't be more than 1 loadTSUTC for a postId since that is generated
  # on our side, but I wanted to handle that since it is part of the key
  data = sorted(data, key=lambda x: x.loadTSUTC)[::-1]
  for d in data:
    if d.postId not in postIds:
      postIds.add(d.postId)
      newData.append(d)
  return newData


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
  no built in way to get responses with batch_writer https://peppydays.medium.com/getting-response-of-aws-dynamodb-batchwriter-request-2aa3f81019fa

  :param table: boto3 table object
  :param data: list[Row[schema]]
  :param schema: OrderedDict containing the dynamodb schema (dynamo technically schema-less)
  :return: None
  """
  columns = schema.keys()
  with table.batch_writer() as batch:
    for i in range(len(data)):  # for each row obtained
      batch.put_item(
        Item = json.loads(json.dumps({k:getattr(data[i], k) for k in columns}), parse_float=Decimal) # helps with parsing float to Decimal
      )