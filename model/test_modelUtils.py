import boto3
import modelUtils as mu
from datetime import datetime, timedelta
from moto import mock_s3
import pytest
import os
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def test_dateToStr():
  d = datetime(2023, 5, 1, 10, 00, 00).date()
  assert mu.dateToStr(d) == d.strftime('%Y-%m-%d')


def test_daysUntilNow():
  now = datetime.now()
  days = list(range(5))[::-1]
  sampleDates = [mu.dateToStr(now - timedelta(days=i)) for i in days]
  returnedDates = mu.daysUntilNow(startingDate=datetime.strptime(sampleDates[0], '%Y-%m-%d').date())
  assert sampleDates == returnedDates


def test_flattenItems():
  a = [[1,2,3], [4,5,6]]
  assert [1,2,3,4,5,6] == mu.flattenItems(a)


def test_getTarget():
  uniqueHotPostIds = {'123456', '123457'}
  assert mu.getTarget('123456', uniqueHotPostIds) == 1
  assert mu.getTarget('212341', uniqueHotPostIds) == 0


@mock_s3
def test_getLatestModel():
  s3 = boto3.client('s3', region_name='us-east-2')
  file1 = os.path.join(THIS_DIR, 'pickledModels/Reddit_model_20230503-235329_GBM.sav')
  file1Name = 'models/Reddit_model_20230503-235329_GBM.sav'
  file2 = os.path.join(THIS_DIR, 'pickledModels/Reddit_model_20230414-061009_LR.sav')
  file2Name = 'models/Reddit_model_20230414-061009_LR.sav'
  bucketName = "test-bucket"
  # take this file and put it in the mock bucket
  s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
  with open(file1, "rb") as f:
    s3.upload_fileobj(f, bucketName, file1Name)
  with open(file2, "rb") as f:
    s3.upload_fileobj(f, bucketName, file2Name)
  from modelUtils import getLatestModel, getModel
  _, latestModelLoc = getLatestModel(bucketName=bucketName)
  assert latestModelLoc == file1Name  # latest model


@mock_s3
def test_getModel():
  s3 = boto3.client('s3', region_name='us-east-2')
  localFileName = os.path.join(THIS_DIR, 'pickledModels/test_latestModel.sav')
  bucketName = "test-bucket"
  fileName = "test-file.sav"
  # take this file and put it in the mock bucket
  s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
  with open(localFileName, "rb") as f:
    s3.upload_fileobj(f, bucketName, fileName)
  from modelUtils import getModel

  getModel(modelName=fileName, bucketName=bucketName, modelSaveLoc=localFileName)

