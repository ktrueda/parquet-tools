from argparse import ArgumentParser, Namespace
from logging import getLogger

import pandas as pd
import pyarrow.parquet as pq
from tabulate import tabulate

from .utils import get_aws_session, is_s3_file, fetch_s3_to_tmp

logger = getLogger(__name__)


def configure_parser(paser: ArgumentParser) -> ArgumentParser:
    paser.add_argument('file',
                       metavar='FILE',
                       type=str,
                       help='''
                       The parquet file to print to stdout.
                       e.g. ./target.parquet or s3://bucker-name/target.parquet
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
                       default='default',
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')

    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    if is_s3_file(args.file):
        with fetch_s3_to_tmp(aws_session=get_aws_session(args.awsprofile),
                             s3uri=args.file) as localfile:
            _execute(
                filename=localfile,
                format=args.format,
                head=args.head,
                columns=args.columns
            )
    else:
        _execute(
            filename=args.file,
            format=args.format,
            head=args.head,
            columns=args.columns
        )


def _execute(filename: str, format: str, head: int, columns: list) -> None:
    df: pd.DataFrame = pq.read_table(filename).to_pandas()
    # head
    df_head: pd.DataFrame = df.head(head) if head > 0 else df
    # select columns
    df_select: pd.DataFrame = df[columns] if len(columns) else df_head
    print(tabulate(df_select, df_select.columns, tablefmt=format, showindex=False))
