import pytest
from parquet_tools.commands.utils import (FileNotFoundException,
                                          InvalidCommandExcpetion,
                                          LocalParquetFile, S3ParquetFile,
                                          _resolve_wildcard,
                                          get_datafame_from_objs)
import pandas as pd


class TestLocalParquetFile:

    @pytest.mark.parametrize('pf, expected', [
        (LocalParquetFile(path='./test.parquet'), False),
        (LocalParquetFile(path='./*'), True),
    ])
    def test_is_wildcard(self, pf, expected):
        assert pf.is_wildcard() == expected

    @pytest.mark.parametrize('pf, expected', [
        (
            LocalParquetFile(path='./tests/*.parquet'), [
                LocalParquetFile('./tests/test1.parquet'),
                LocalParquetFile('./tests/test2.parquet')
            ]
        ),
    ])
    def test_resolve_wildcard(self, pf, expected):
        assert pf.resolve_wildcard() == expected


class TestS3ParquetFile:

    @pytest.mark.parametrize('bucket, key, expected', [
        ('foo', 'tests/*.parquet', False),
        ('foo', 'tests/*', True),
        ('foo', '*', True),
        ('foo', 'foo.csv', True),
    ])
    def test_validation(self, aws_session, bucket, key, expected):
        if not expected:
            with pytest.raises(InvalidCommandExcpetion):
                S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        else:
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)

    def test_resolve_wildcard(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        actual = S3ParquetFile(aws_session=aws_session,
                               bucket=bucket,
                               key='*').resolve_wildcard()
        assert len(actual) == 1
        assert actual[0].bucket == bucket
        assert actual[0].key == key

    def test_resolve_wildcard_not_found(self, aws_session, parquet_file_s3_1):
        bucket, _ = parquet_file_s3_1
        actual = S3ParquetFile(aws_session=aws_session,
                               bucket=bucket,
                               key='not_found*').resolve_wildcard()
        assert len(actual) == 0

    def test_local_path(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        with S3ParquetFile(aws_session=aws_session,
                           bucket=bucket,
                           key=key).get_local_path() as localfiles:

            assert localfiles.endswith('.parquet')


class TestResolveWildcard:
    def test_single_localfile(self):
        actual = _resolve_wildcard(
            LocalParquetFile(path='./tests/test1.parquet')
        )

        assert len(actual) == 1
        assert actual[0].path == './tests/test1.parquet'

    def test_multiple_localfile(self):
        actual = _resolve_wildcard(
            LocalParquetFile(path='./tests/*.parquet'),
        )

        assert len(actual) == 2
        assert isinstance(actual[0], LocalParquetFile)
        assert isinstance(actual[1], LocalParquetFile)

        assert {a.path for a in actual} == {
            './tests/test1.parquet',
            './tests/test2.parquet',
        }

    def test_concrete_local_not_found(self):
        assert _resolve_wildcard(
            LocalParquetFile(path='not_found.parquet'),
        ) == [
            LocalParquetFile(path='not_found.parquet'),
        ]

    def test_wildcard_local_not_found(self):
        assert _resolve_wildcard(
            LocalParquetFile(path='not_found*'),
        ) == []

    def test_single_s3file(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        actual = _resolve_wildcard(
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        )
        assert len(actual) == 1
        assert isinstance(actual[0], S3ParquetFile)
        assert actual[0].key.endswith('.parquet')

    def test_s3_not_found(self, aws_session, parquet_file_s3_1):
        bucket, _ = parquet_file_s3_1
        assert _resolve_wildcard(
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key='not_found.parquet')
        ) == [
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key='not_found.parquet')
        ]

    def test_wildcard_s3_not_found(self, aws_session, parquet_file_s3_1):
        bucket, _ = parquet_file_s3_1
        assert _resolve_wildcard(
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key='not_found*')
        ) == []


class TestGetDataframeFromObjs:

    def test_local_single_file(self):
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/test1.parquet')
        ]) as df:

            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3

    def test_local_double_file(self):
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/test1.parquet'),
            LocalParquetFile(path='./tests/test2.parquet')
        ]) as df:

            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 2

    def test_local_wildcard(self):
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/*'),
        ]) as df:

            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 3

    def test_s3_single_file(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        with get_datafame_from_objs([
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        ]) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 1

    def test_s3_double_file(self, aws_session, parquet_file_s3_1, parquet_file_s3_2):
        bucket_1, key_1 = parquet_file_s3_1
        bucket_2, key_2 = parquet_file_s3_2
        with get_datafame_from_objs([
            S3ParquetFile(aws_session=aws_session, bucket=bucket_1, key=key_1),
            S3ParquetFile(aws_session=aws_session, bucket=bucket_2, key=key_2)
        ]) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 2

    def test_s3_wildcard_file(self, aws_session, parquet_file_s3_1, parquet_file_s3_2):
        bucket, _ = parquet_file_s3_1
        with get_datafame_from_objs([
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key='*'),
        ]) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 2

    def test_local_and_s3_files(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/test1.parquet'),
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        ]) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * 2

    def test_local_and_s3_wildcard_files(self, aws_session, parquet_file_s3_1, parquet_file_s3_2):
        bucket, _ = parquet_file_s3_1
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/*'),  # hit local 3 files
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key='*')  # hit 2 files on s3
        ]) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3 * (3 + 2)

    def test_head_early_stopping(self):
        with get_datafame_from_objs([
            LocalParquetFile(path='./tests/*'),
        ], head=2) as df:
            assert isinstance(df, pd.core.frame.DataFrame)
            assert len(df) == 3
