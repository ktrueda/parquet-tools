from os.path import dirname

import numpy as np
import pandas as pd


def get_test_dataframe():
    return pd.DataFrame(
        {'one': [-1, np.nan, 2.5],
         'two': ['foo', 'bar', 'baz'],
         'three': [True, False, True]}
    )


def write_test_dataframes():
    df = get_test_dataframe()
    tests_dir = dirname(__file__)
    df.to_parquet(f'{tests_dir}/test1.parquet')
    df.to_parquet(f'{tests_dir}/test2.parquet')


if __name__ == '__main__':
    write_test_dataframes()
