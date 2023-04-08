import utils
import praw
import boto3
from tabledefinitions import hotTableDefinition
from tabledefinitions import risingTableDefinition


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
  risingSchema = risingTableDefinition.schema
  topN = 25
  view = 'rising'
  risingData = utils.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=risingSchema, topN=topN)

  # Push to DynamoDB
  risingRawTableDefinition = risingTableDefinition.tableDefinition
  risingTable = utils.getOrCreateTable(risingRawTableDefinition, dynamodb_resource)
  utils.batchWriter(risingTable, risingData, risingSchema)

  # Get Hot Reddit data
  hotSchema = hotTableDefinition.schema
  topN = 3
  view = 'hot'
  hotData = utils.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=hotSchema, topN=topN)

  # Push to DynamoDB
  hotRawTableDefinition = hotTableDefinition.tableDefinition
  hotTable = utils.getOrCreateTable(hotRawTableDefinition, dynamodb_resource)
  utils.batchWriter(hotTable, hotData, hotSchema)

  return 200
