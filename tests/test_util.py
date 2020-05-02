from parquet_tools.commands.utils import fetch_s3_to_tmp


def test_fetch_s3_to_tmp(aws_session, parquet_file_s3):
    with fetch_s3_to_tmp(aws_session, parquet_file_s3) as f:
        assert f is not None
