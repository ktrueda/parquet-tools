import boto3
import pytest

try:
    # Moto 4.x
    from moto import mock_s3
except ImportError:
    # Moto 5.x
    from moto import mock_aws as mock_s3

@pytest.fixture
def aws_session():
    with mock_s3():
        yield boto3.Session()

@pytest.fixture
def aws_s3_bucket(aws_session):
    aws_session.resource('s3', region_name='us-east-1')\
        .create_bucket(Bucket='mybucket')
    return 'mybucket'
