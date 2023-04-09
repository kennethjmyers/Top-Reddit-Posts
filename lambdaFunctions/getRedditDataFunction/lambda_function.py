import utils
import tableDefinition
import praw
import boto3


dynamodb_resource = boto3.resource('dynamodb')


def lambda_handler(event, context):
  # Initializations
  subreddit = "pics"
  cfg_file = utils.findConfig()
  cfg = utils.parseConfig(cfg_file)

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

  # Get Rising Reddit data
  schema = tableDefinition.schema
  topN = 25
  view = 'rising'
  risingData = utils.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=schema, topN=topN)

  # Push to DynamoDB
  tableName = view
  risingRawTableDefinition = tableDefinition.getTableDefinition(tableName)
  risingTable = utils.getOrCreateTable(risingRawTableDefinition, dynamodb_resource)
  utils.batchWriter(risingTable, risingData, schema)

  # Get Hot Reddit data
  schema = tableDefinition.schema
  topN = 3
  view = 'hot'
  hotData = utils.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=schema, topN=topN)

  # Push to DynamoDB
  tableName = view
  hotTableDefinition = tableDefinition.getTableDefinition(tableName)
  hotTable = utils.getOrCreateTable(hotTableDefinition, dynamodb_resource)
  utils.batchWriter(hotTable, hotData, schema)

  return 200
