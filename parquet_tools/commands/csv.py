from argparse import ArgumentParser, Namespace

import pandas as pd
import pyarrow.parquet as pq


def configure_parser(paser: ArgumentParser) -> ArgumentParser:
    paser.add_argument('file',
                       metavar='FILE',
                       type=str,
                       help='The parquet file to print to stdout')
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
    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    _execute(
        filename=args.file,
        head=args.head,
        columns=args.columns,
    )


def _execute(filename: str, head: int, columns: list) -> None:
    df: pd.DataFrame = pq.read_table(filename).to_pandas()
    # head
    df_head: pd.DataFrame = df.head(head) if head > 0 else df
    # select columns
    df_select: pd.DataFrame = df[columns] if len(columns) else df_head
    print(df_select.to_csv(index=None))
