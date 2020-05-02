import boto3
from moto import mock_s3
import pytest


@pytest.fixture
def aws_session():
    mock_s3_server = mock_s3()
    mock_s3_server.start()
    yield boto3.Session()
    mock_s3_server.stop()


@pytest.fixture
def aws_s3_bucket(aws_session):
    aws_session.resource('s3', region_name='us-east-1')\
        .create_bucket(Bucket='mybucket')
    return 'mybucket'
