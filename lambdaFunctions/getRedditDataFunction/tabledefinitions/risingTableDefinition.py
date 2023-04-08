from collections import OrderedDict


schema = OrderedDict()
schema["postId"] = "S"
schema["subreddit"] = "S"
schema["title"] = "S"
schema["createdUTC"] = "S"
schema["timeElapsedMin"] = "N"
schema["score"] = "N"
schema["numComments"] = "N"
schema["upvoteRatio"] = "N"
schema["numGildings"] = "N"
schema["loadTimeUTC"] = "S"
schema["loadDateUTC"] = "S"

tableName = "rising"

tableDefinition = dict(
  AttributeDefinitions=[
    {
      'AttributeName': k,
      'AttributeType': schema[k]
    } for k in ['postId', 'loadTimeUTC', 'subreddit']  # only need to define the ones that are used in key and sort
  ],
  TableName=tableName,
  KeySchema=[
    {
      'AttributeName': 'postId',
      'KeyType': 'HASH'
    },
    {
      'AttributeName': 'loadTimeUTC',
      'KeyType': 'RANGE'

    }
  ],
  GlobalSecondaryIndexes=[  # I wanted to future proof other ways I might look at the table (by subreddit)
      {
          'IndexName': 'bySubreddit',
          'KeySchema': [
              {
                  'AttributeName': 'subreddit',
                  'KeyType': 'HASH'
              },
              {
                  'AttributeName': 'loadTimeUTC',
                  'KeyType': 'RANGE'
              },
          ],
          'Projection': {
              'ProjectionType': 'KEYS_ONLY',
          },
          'ProvisionedThroughput': {  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html#GSI.ThroughputConsiderations
              'ReadCapacityUnits': 1,  # 1 = 4KB/s I think
              'WriteCapacityUnits': 1  # 1 = 1KB/s
          }
      },
  ],
  BillingMode='PROVISIONED',  # recommended for consistent work
  ProvisionedThroughput={  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#default-limits-throughput-capacity-modes
      'ReadCapacityUnits': 4,
      'WriteCapacityUnits': 4
  },
  TableClass='STANDARD',
  DeletionProtectionEnabled=False
)
