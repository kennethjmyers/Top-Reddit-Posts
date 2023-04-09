from collections import OrderedDict


# schema is mainly needed for defining columns and what the column types should be if building additional indices
schema = OrderedDict()
schema["loadDateUTC"] = "S"
schema["loadTimeUTC"] = "S"
schema["loadTSUTC"] = "S"
schema["postId"] = "S"
schema["subreddit"] = "S"
schema["title"] = "S"
schema["createdTSUTC"] = "S"
schema["timeElapsedMin"] = "N"
schema["score"] = "N"
schema["numComments"] = "N"
schema["upvoteRatio"] = "N"
schema["numGildings"] = "N"

baseTableDefinition = dict(
  AttributeDefinitions=[
    {
      'AttributeName': k,
      'AttributeType': schema[k]
    } for k in ['postId', 'loadDateUTC', 'loadTimeUTC', 'loadTSUTC']  # only need to define the ones that are used in key and sort
  ],
  KeySchema=[
    {
      'AttributeName': 'postId',
      'KeyType': 'HASH'
    },
    {
      'AttributeName': 'loadTSUTC',
      'KeyType': 'RANGE'

    }
  ],
  GlobalSecondaryIndexes=[  # I wanted to future proof other ways I might look at the table (by subreddit)
      {
          'IndexName': 'byLoadDate',
          'KeySchema': [
              {
                  'AttributeName': 'loadDateUTC',
                  'KeyType': 'HASH'
              },
              {
                  'AttributeName': 'loadTimeUTC',
                  'KeyType': 'RANGE'
              },
          ],
          'Projection': {
              'ProjectionType': 'INCLUDE',
              'NonKeyAttributes': [
                    'timeElapsedMin',
                ]
          },
          'ProvisionedThroughput': {  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html#GSI.ThroughputConsiderations
              'ReadCapacityUnits': 1,  # 1 = 4KB/s I think
              'WriteCapacityUnits': 1  # 1 = 1KB/s
          }
      },
  ],
  BillingMode='PROVISIONED',  # recommended for consistent work
  ProvisionedThroughput={  # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#default-limits-throughput-capacity-modes
      'ReadCapacityUnits': 1,
      'WriteCapacityUnits': 1
  },
  TableClass='STANDARD',
  DeletionProtectionEnabled=False
)

def getTableDefinition(tableName, tableDefintion = baseTableDefinition):
  tableDefintion['TableName'] = tableName
  return tableDefintion
