import sys
from argparse import ArgumentParser, Namespace
from functools import reduce
from typing import List

import pandas as pd
import pyarrow.parquet as pq

from .utils import (FileNotFoundException, InvalidCommandExcpetion,
                    ParquetFile, get_filepaths_from_objs, to_parquet_file)


def configure_parser(paser: ArgumentParser) -> ArgumentParser:
    paser.add_argument('file',
                       metavar='FILE',
                       type=str,
                       nargs='+',
                       help='''The parquet file to read.
                       e.g. ./target.parquet or s3://bucket-name/target.parquet or s3://bucket-name/*''')
    paser.add_argument('--columns', '-c',
                       type=lambda s: s.split(","),
                       default=[],
                       help='''Show only the given column, can be specified more than once.
                             e.g. --columns email,name
                             ''')
    paser.add_argument('--head', '-n',
                       type=int,
                       required=False,
                       default=-1,
                       help='Show only head record(default:infinity)')
    paser.add_argument('--awsprofile',
                       type=str,
                       required=False,
                       default='default',
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')
    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    try:
        pfs: List[ParquetFile] = [
            to_parquet_file(file_exp=f, awsprofile=args.awsprofile)
            for f in args.file]
        with get_filepaths_from_objs(pfs) as localfiles:
            if len(localfiles) == 0:
                raise FileNotFoundException('File matching that expression not found.')
            _execute(
                filenames=localfiles,
                head=args.head,
                columns=args.columns
            )
    except InvalidCommandExcpetion as e:
        print(str(e), file=sys.stderr)
    except FileNotFoundException as e:
        print(str(e), file=sys.stderr)


def _execute(filenames: List[str], head: int, columns: list) -> None:
    df: pd.DataFrame = reduce(lambda x, y: pd.concat([x, y]),
                              [pq.read_table(fn).to_pandas() for fn in filenames])
    # head
    df_head: pd.DataFrame = df.head(head) if head > 0 else df
    # select columns
    df_select: pd.DataFrame = df[columns] if len(columns) else df_head
    print(df_select.to_csv(index=None))
