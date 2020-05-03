from parquet_tools.commands.utils import (
    LocalParquetFile,
    S3ParquetFile,
    get_filepaths_from_objs,
    InvalidCommandExcpetion,
    FileNotFoundException)
import pytest


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


class TestGetFilePathsFromObjs:
    def test_single_localfile(self):
        with get_filepaths_from_objs([
            LocalParquetFile(path='./tests/test1.parquet')
        ]) as localfiles:
            assert localfiles == ['./tests/test1.parquet']

    def test_multiple_localfile(self):
        with get_filepaths_from_objs([
            LocalParquetFile(path='./tests/test1.parquet'),
            LocalParquetFile(path='./tests/*.parquet'),
        ]) as localfiles:
            assert localfiles == [
                './tests/test1.parquet',
                './tests/test1.parquet',
                './tests/test2.parquet',
            ]

    def test_concrete_local_not_found(self):
        with pytest.raises(FileNotFoundException):
            with get_filepaths_from_objs([
                LocalParquetFile(path='not_found.parquet'),
            ]) as localfiles:
                pass

    def test_wildcard_local_not_found(self):
        with get_filepaths_from_objs([
            LocalParquetFile(path='not_found*'),
        ]) as localfiles:
            assert len(localfiles) == 0

    def test_single_s3file(self, aws_session, parquet_file_s3_1):
        bucket, key = parquet_file_s3_1
        with get_filepaths_from_objs([
            S3ParquetFile(aws_session=aws_session, bucket=bucket, key=key)
        ]) as localfiles:
            assert len(localfiles) == 1
            assert localfiles[0].endswith('.parquet')

    def test_multiple_s3file(self, aws_session, parquet_file_s3_1, parquet_file_s3_2):
        bucket_1, key_1 = parquet_file_s3_1
        bucket_2, key_2 = parquet_file_s3_2
        with get_filepaths_from_objs([
            S3ParquetFile(aws_session=aws_session, bucket=bucket_1, key=key_1),
            S3ParquetFile(aws_session=aws_session, bucket=bucket_2, key=key_2),
        ]) as localfiles:
            assert len(localfiles) == 2
            assert localfiles[0].endswith('.parquet')
            assert localfiles[1].endswith('.parquet')
            assert localfiles[0] != localfiles[1]

    def test_s3_not_found(self, aws_session, parquet_file_s3_1):
        bucket, _ = parquet_file_s3_1
        with pytest.raises(FileNotFoundException):
            with get_filepaths_from_objs([
                S3ParquetFile(aws_session=aws_session, bucket=bucket, key='not_found.parquet')
            ]) as localfiles:
                pass
