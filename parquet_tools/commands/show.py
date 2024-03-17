import os
import sys
from argparse import ArgumentParser, Namespace
from logging import getLogger
from typing import List, Optional

import pandas as pd
from tabulate import tabulate

from .utils import (FileNotFoundException, InvalidCommandExcpetion,
                    ParquetFile, get_datafame_from_objs,
                    to_parquet_file)

logger = getLogger(__name__)


def configure_parser(paser: ArgumentParser) -> ArgumentParser:
    paser.add_argument('file',
                       metavar='FILE',
                       type=str,
                       nargs='+',
                       help='''
                       The parquet file to print to stdout.
                       e.g. ./target.parquet or s3://bucket-name/target.parquet or s3://bucket-name/*
                       ''')
    paser.add_argument('--format', '-f',
                       action='store',
                       required=False,
                       default='psql',
                       choices=[
                           'psql', 'github'
                       ],
                       help='''Table format(default: psql).
                             ''')
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
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')
    paser.add_argument('--endpoint-url',
                       type=str,
                       required=False,
                       default=os.environ.get('ENDPOINT_URL'),
                       help='A custom S3 endpoint URL')

    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    try:
        pfs: List[ParquetFile] = [
            to_parquet_file(file_exp=f, awsprofile=args.awsprofile, endpoint_url=args.endpoint_url)
            for f in args.file]
        with get_datafame_from_objs(pfs, args.head) as df:
            if df is None:
                raise FileNotFoundException('File matching that expression not found.')
            _execute(
                df=df,
                format=args.format,
                head=args.head,
                columns=args.columns
            )
    except InvalidCommandExcpetion as e:
        print(str(e), file=sys.stderr)
    except FileNotFoundException as e:
        print(str(e), file=sys.stderr)


def _execute(df: pd.DataFrame, format: str, head: int, columns: list) -> None:
    # head
    df_head: pd.DataFrame = df.head(head) if head > 0 else df
    # select columns
    df_select: pd.DataFrame = df_head[columns] if len(columns) else df_head
    print(tabulate(df_select, df_select.columns, tablefmt=format, showindex=False))
