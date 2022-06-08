import pytest
from parquet_tools.commands.inspect import _execute_detail, _execute_simple
import pyarrow as pa
import pandas as pd
from tempfile import TemporaryDirectory
import numpy as np


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


def test_excute_detail(parquet_file):
    _execute_detail(
        parquet_file
    )
    # not raise error


def test_excute_simple(capfd, parquet_file):
    _execute_simple(
        parquet_file
    )
    out, err = capfd.readouterr()

    assert err == ''
    assert out == '''
############ file meta data ############
created_by: parquet-cpp-arrow version 5.0.0
num_columns: 3
num_rows: 3
num_row_groups: 1
format_version: 1.0
serialized_size: 2222


############ Columns ############
one
two
three

############ Column(one) ############
name: one
path: one
max_definition_level: 1
max_repetition_level: 0
physical_type: DOUBLE
logical_type: None
converted_type (legacy): NONE
compression: SNAPPY (space_saved: -4%)

############ Column(two) ############
name: two
path: two
max_definition_level: 1
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8
compression: SNAPPY (space_saved: -5%)

############ Column(three) ############
name: three
path: three
max_definition_level: 1
max_repetition_level: 0
physical_type: BOOLEAN
logical_type: None
converted_type (legacy): NONE
compression: SNAPPY (space_saved: -5%)

'''
