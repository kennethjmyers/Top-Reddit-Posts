from collections import OrderedDict


hotSchema = OrderedDict()
hotSchema["postId"] = "S"
hotSchema["subreddit"] = "S"
hotSchema["title"] = "S"
hotSchema["createdUTC"] = "S"
hotSchema["timeElapsedMin"] = "N"
hotSchema["score"] = "N"
hotSchema["numComments"] = "N"
hotSchema["upvoteRatio"] = "N"
hotSchema["numGildings"] = "N"
hotSchema["loadTimeUTC"] = "S"
hotSchema["loadDateUTC"] = "S"

tableName = "hotRaw"

hotRawTableDefinition = dict(
  AttributeDefinitions=[
    {
      'AttributeName': k,
      'AttributeType': hotSchema[k]
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
      'ReadCapacityUnits': 2,
      'WriteCapacityUnits': 2
  },
  TableClass='STANDARD',
  DeletionProtectionEnabled=False
)
