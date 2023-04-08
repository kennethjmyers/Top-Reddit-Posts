import utils
import praw
import boto3
import tabledefinitions

dynamodb_resource = boto3.resource('dynamodb')

def lambda_handler(event, context):
  # Get Reddit Data
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

  risingData = utils.getRising(reddit, subreddit, 25)

  # Push to DynamoDB
  risingStagingTableDefinition = tabledefinitions.risingTableDefinition.risingStagingTableDefinition
  risingSchema = tabledefinitions.risingTableDefinition.risingSchema
  risingTable = utils.getOrCreateTable(risingStagingTableDefinition, dynamodb_resource)
  utils.batchWriter(risingTable, risingData, risingSchema)
  return 200
