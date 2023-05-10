import redditUtils as ru
import configUtils as cu
import tableDefinition
import praw
import boto3


dynamodb_resource = boto3.resource('dynamodb')


def lambda_handler(event, context):
  # Initializations
  subreddits = ["pics", "memes", "gaming", "worldnews", "news", "aww", "funny", "todayilearned", "movies"]

  # cfg_file = cu.findConfig()
  cfg_file = 's3://data-kennethmyers/reddit.cfg'
  cfg = cu.parseConfig(cfg_file)

  CLIENTID = cfg['reddit_api']['CLIENTID']
  CLIENTSECRET = cfg['reddit_api']['CLIENTSECRET']
  PASSWORD = cfg['reddit_api']['PASSWORD']
  USERNAME = cfg['reddit_api']['USERNAME']

  reddit = praw.Reddit(
    client_id=f"{CLIENTID}",
    client_secret=f"{CLIENTSECRET}",
    password=f"{PASSWORD}",
    user_agent=f"Post Extraction (by u/{USERNAME})",
    username=f"{USERNAME}",
  )

  for subreddit in subreddits:
    print(f"Gathering data for {subreddit}")
    # Get Rising Reddit data
    print("\tGetting Rising Data")
    schema = tableDefinition.schema
    topN = 25
    view = 'rising'
    risingData = ru.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=schema, topN=topN)
    risingData = ru.deduplicateRedditData(risingData)

    # Push to DynamoDB
    tableName = view
    risingRawTableDefinition = tableDefinition.getTableDefinition(tableName)
    risingTable = ru.getOrCreateTable(risingRawTableDefinition, dynamodb_resource)
    ru.batchWriter(risingTable, risingData, schema)

    # Get Hot Reddit data
    print("\tGetting Hot Data")
    schema = tableDefinition.schema
    topN = 3
    view = 'hot'
    hotData = ru.getRedditData(reddit=reddit, subreddit=subreddit, view=view, schema=schema, topN=topN)
    hotData = ru.deduplicateRedditData(hotData)

    # Push to DynamoDB
    tableName = view
    hotTableDefinition = tableDefinition.getTableDefinition(tableName)
    hotTable = ru.getOrCreateTable(hotTableDefinition, dynamodb_resource)
    ru.batchWriter(hotTable, hotData, schema)

  return 200
