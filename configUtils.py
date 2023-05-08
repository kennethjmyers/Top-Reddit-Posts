from configparser import ConfigParser
import json
import boto3
import os


def findConfig() -> str:
  """
  Finds config file locally

  :return: string of config file location
  """
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


DEFAULT_KEYS = {
  'reddit_api': ['CLIENTID', 'CLIENTSECRET', 'PASSWORD', 'USERNAME'],
  'S3_access': ['ACCESSKEY', 'SECRETKEY', ],
  'Discord': ['BOTTOKEN', 'MYSNOWFLAKEID', 'CHANNELSNOWFLAKEID'],
  'Postgres': ['USERNAME', 'PASSWORD', 'HOST', 'PORT', 'DATABASE']
}


def parseConfig(
  cfgFile: str,
  keysToRead: dict = None
) -> dict:
  """
  Read in the config data from a location to a dictionary and return that dictionary.

  :param cfgFile: location of config file. Can be an S3 location
  :param keysToRead:
  :return: config dictionary
  """
  if keysToRead is None:
    keysToRead = DEFAULT_KEYS
  parser = ConfigParser()
  cfg = dict()

  if cfgFile[:2].lower() == 's3':
    s3 = boto3.client('s3')
    pathSplit = cfgFile.replace('s3://', '').split('/')
    bucket = pathSplit[0]
    objLoc = '/'.join(pathSplit[1:])
    obj = s3.get_object(Bucket=bucket, Key=objLoc)
    _ = parser.read_string(obj['Body'].read().decode())
  else:
    _ = parser.read(cfgFile)
  for k, vList in keysToRead.items():
    for v in vList:
      cfg[v] = json.loads(parser.get(k, v))  # json helps with list conversion
  return cfg
