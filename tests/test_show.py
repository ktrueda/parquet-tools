import argparse

import pandas as pd
import numpy as np
import pytest
from parquet_tools.commands.show import _execute, configure_parser


@pytest.fixture
def parser():
    parser = argparse.ArgumentParser()
    return configure_parser(parser)


@pytest.mark.parametrize('arg, valid,exptected', [
    ('', False, {}),
    # most simple args
    (
        'file1.parquet', True,
        {
            'columns': [],
            'file': ['file1.parquet'],
            'format': 'psql',
            'head': -1,
            'awsprofile': None,
            'endpoint_url': None
        }
    ),
    # most complex one
    (
        '--columns col1,col2 --format github --head 100 file1.parquet file2.parquet', True,
        {
            'columns': ['col1', 'col2'],
            'file': ['file1.parquet', 'file2.parquet'],
            'format': 'github',
            'head': 100,
            'awsprofile': None,
            'endpoint_url': None
        }
    ),
    # file is on S3
    (
        '--awsprofile user1 s3://bucket-name/file1.parquet', True,
        {
            'columns': [],
            'file': ['s3://bucket-name/file1.parquet'],
            'format': 'psql',
            'head': -1,
            'awsprofile': 'user1',
            'endpoint_url': None
        }
    ),
    # empty columns
    (
        '--columns file1.parquet', False, {}
    ),
    # not integer head
    (
        '--head foo file1.parquet', False, {}
    ),
    # not supported format
    (
        '--format foo file1.parquet', False, {}
    ),

])
def test_configure_parser(parser, arg, valid, exptected):
    if not valid:
        with pytest.raises(SystemExit):
            assert parser.parse_args(arg.split())

    if valid:
        actual = vars(parser.parse_args(arg.split()))
        assert {k: v for k, v in actual.items() if k != 'handler'} == exptected


def test_excute(capfd, parquet_file):
    _execute(
        df=pd.DataFrame(
            {'one': [-1, np.nan, 2.5],
             'two': ['foo', 'bar', 'baz'],
             'three': [True, False, True]}
        ),
        format='psql',
        head=2,
        columns=['one', 'three']
    )
    out, err = capfd.readouterr()

    assert out is not None
    assert err == ''
    assert 'foo' not in out, 'Column two should not be output'
    assert '2.5' not in out, 'Row 3 should not be output'

