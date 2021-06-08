import glob
from abc import ABC, abstractmethod
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from functools import reduce
from logging import getLogger
from pathlib import Path
from sys import stderr
from tempfile import TemporaryDirectory
from typing import Iterator, List, Optional, Union
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import lib as pyarrowlib
from halo import Halo

logger = getLogger(__name__)


class InvalidCommandExcpetion(Exception):
    '''Exception for invalid command. Argment parser raises this Exception.
    '''
    pass


class FileNotFoundException(Exception):
    pass


class ParquetFile(ABC):
    '''Abstract ParquetFile.
    One object does not correspond one parquet file but one expression about file
    such as ./target.parquet, ./*.parquet, s3://bucket/foo.parquet or s3://bucket/*
    '''

    def __post_init__(self):
        self.validation()

    def validation(self) -> None:
        '''validate properties
        '''
        pass

    @abstractmethod
    def is_wildcard(self) -> bool:
        '''Return if this object correspond one or more object.
        '''
        raise NotImplementedError()

    @abstractmethod
    def resolve_wildcard(self) -> List['ParquetFile']:
        '''Return concrete Parquert file objects.
        '''
        raise NotImplementedError()

    @contextmanager
    @abstractmethod
    def get_local_path(self) -> Iterator[str]:
        '''Return local file path.
        If call this function of S3ParquetFile, return the path of downloaded file.
        '''
        raise NotImplementedError()

    @contextmanager
    def get_dataframe(self) -> pd.DataFrame:
        with self.get_local_path() as local_path:
            try:
                yield pq.read_table(local_path).to_pandas()
            except pyarrowlib.ArrowInvalid:
                print(f"File({local_path}) cannot be read as parquet.", file=stderr)
                yield None


@dataclass
class LocalParquetFile(ParquetFile):
    '''Parquet file object on local disk
    '''
    path: str

    def is_wildcard(self) -> bool:
        return '*' in self.path

    def resolve_wildcard(self) -> List[ParquetFile]:
        return sorted(
            [LocalParquetFile(f) for f in glob.glob(self.path)],
            key=lambda x: x.path
        )

    @contextmanager
    def get_local_path(self) -> Iterator[str]:
        if self.is_wildcard():
            raise Exception('Please resolve first.')
        if not Path(self.path).exists():
            raise FileNotFoundException(f'File({self.path}) not found')
        yield self.path


@dataclass
class S3ParquetFile(ParquetFile):
    '''Parquet file object on S3
    '''
    aws_session: boto3.Session
    bucket: str
    key: str

    def validation(self):
        ''' key can have *. But it must be last of the string.
        '''
        if self.is_wildcard() and not self.key.index('*') in (-1, len(self.key) - 1):
            raise InvalidCommandExcpetion('You can use * only end of the path')

    def is_wildcard(self) -> bool:
        return '*' in self.key

    def resolve_wildcard(self) -> List[ParquetFile]:
        list_res = self.aws_session.client('s3')\
            .list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.key[:-1]  # remove *
        )
        if list_res['IsTruncated']:
            raise Exception(f'Too much file match s3://{self.bucket}/{self.key}')

        if list_res['KeyCount'] == 0:
            return []
        keys = [e['Key'] for e in list_res['Contents']]
        return sorted(
            [S3ParquetFile(aws_session=self.aws_session, bucket=self.bucket, key=key) for key in keys],
            key=lambda x: x.key
        )

    @contextmanager
    def get_local_path(self) -> Iterator[str]:
        if self.is_wildcard():
            raise Exception('Please resolve first.')
        with TemporaryDirectory() as tmp_path:
            localfile = f'{tmp_path}/{uuid4()}.parquet'
            logger.info(f'Download stat parquet file on s3://{self.bucket}/{self.key} -> {localfile}')
            try:
                with Halo(text='Downloading from s3', spinner='dots', stream=stderr) as spinner:
                    self.aws_session.resource('s3')\
                        .meta.client.download_file(self.bucket, self.key, localfile)
                    spinner.info(f's3://{self.bucket}/{self.key} => {localfile}')
            except Exception:
                raise FileNotFoundException(f's3://{self.bucket}/{self.key} not found or cannot access')
            else:
                yield localfile


def get_aws_session(profile_name: Optional[str]) -> boto3.Session:
    return boto3.Session(profile_name=profile_name)


def _is_s3_file(filename: str) -> bool:
    return filename[:5] == 's3://'


def to_parquet_file(file_exp: str, awsprofile: Optional[str]) -> ParquetFile:
    '''Transform file_exp to ParquetFile object.
    '''
    if _is_s3_file(file_exp):
        parsed_url = urlparse(file_exp)
        return S3ParquetFile(
            aws_session=get_aws_session(awsprofile),
            bucket=parsed_url.netloc,
            key=parsed_url.path[1:]
        )
    else:
        return LocalParquetFile(
            path=file_exp
        )


@contextmanager
def get_datafame_from_objs(objs: List[ParquetFile], head: Union[int, float] = None):
    '''Get pandas dataframe of ParquetFile object list.
    '''

    if head is None or head <= 0:
        head = float('inf')

    cumsum_row: int = 0
    dfs: List[pd.DataFrame] = []
    with ExitStack() as stack:
        for obj in objs:
            for pf in _resolve_wildcard(obj):
                df: Optional[pd.DataFrame] = stack.enter_context(pf.get_dataframe())
                if df is None:
                    continue
                cumsum_row += len(df)
                dfs.append(df)

                if cumsum_row >= head:
                    break
            if cumsum_row >= head:
                break
        if dfs:
            yield reduce(lambda x, y: pd.concat([x, y]), dfs)
        else:
            yield None


def _resolve_wildcard(obj: ParquetFile) -> List[ParquetFile]:
    if not obj.is_wildcard():
        return [obj]
    else:
        return obj.resolve_wildcard()
