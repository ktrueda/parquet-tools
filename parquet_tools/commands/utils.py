import boto3
from contextlib import contextmanager
from typing import Iterator
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from logging import getLogger


logger = getLogger(__name__)


def get_aws_session(profile_name: str='default') -> boto3.Session:
    return boto3.Session(profile_name=profile_name)


def is_s3_file(filename: str) -> bool:
    return filename[:5] == 's3://'


@contextmanager
def fetch_s3_to_tmp(aws_session: boto3.Session, s3uri: str) -> Iterator[str]:
    with TemporaryDirectory() as tmp_path:
        localfile = f'{tmp_path}/local.parquet'
        parsed_url = urlparse(s3uri)
        logger.info(f'Download start parquet file on {s3uri} -> {localfile}')
        aws_session.resource('s3')\
            .meta.client.download_file(parsed_url.netloc,
                                       parsed_url.path[1:],
                                       localfile)
        logger.info(f'Download finish parquet file on {s3uri} -> {localfile}')
        yield localfile
