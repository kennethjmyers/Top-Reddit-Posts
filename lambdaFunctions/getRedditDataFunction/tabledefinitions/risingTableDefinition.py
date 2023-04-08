from collections import OrderedDict


risingSchema = OrderedDict()
risingSchema["postId"] = "S"
risingSchema["subreddit"] = "S"
risingSchema["title"] = "S"
risingSchema["createdUTC"] = "S"
risingSchema["timeElapsedMin"] = "N"
risingSchema["score"] = "N"
risingSchema["numComments"] = "N"
risingSchema["upvoteRatio"] = "N"
risingSchema["numGildings"] = "N"
risingSchema["loadTimeUTC"] = "S"
risingSchema["loadDateUTC"] = "S"

tableName = "risingStaging"

risingStagingTableDefinition = dict(
  AttributeDefinitions=[
    {
      'AttributeName': k,
      'AttributeType': risingSchema[k]
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
