import pytest
from parquet_tools.commands.csv import configure_parser, _execute
import argparse
import pyarrow as pa
import pandas as pd
from tempfile import TemporaryDirectory
import numpy as np


@pytest.fixture
def parser():
    parser = argparse.ArgumentParser()
    return configure_parser(parser)


@pytest.fixture
def parquet_file():
    df = pd.DataFrame(
        {'one': [-1, np.nan, 2.5],
         'two': ['foo', 'bar', 'baz'],
         'three': [True, False, True]}
    )
    table = pa.Table.from_pandas(df)
    with TemporaryDirectory() as tmp_path:
        pq_path = f'{tmp_path}/test.pq'
        pa.parquet.write_table(table, pq_path)
        yield pq_path


@pytest.mark.parametrize('arg, valid,exptected', [
    ('', False, {}),
    # most simple args
    (
        'file1.parquet', True,
        {
            'columns': [],
            'file': ['file1.parquet'],
            'head': -1,
            'awsprofile': 'default'
        }
    ),
    # most complex one
    (
        '--columns col1,col2 --head 100 --awsprofile user1 file1.parquet file2.parquet', True,
        {
            'columns': ['col1', 'col2'],
            'file': ['file1.parquet', 'file2.parquet'],
            'head': 100,
            'awsprofile': 'user1'
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
        filenames=[parquet_file],
        head=-1,
        columns=[]
    )
    out, err = capfd.readouterr()
    assert out is not None
    assert err == ''
