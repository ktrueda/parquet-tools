import os
import sys
from argparse import ArgumentParser, Namespace
from typing import List

import pyarrow.parquet as pq
from colorama import Fore, Style
from parquet_tools.parquet.reader import get_filemetadata

from .utils import FileNotFoundException, ParquetFile, to_parquet_file


def dedent(text: str) -> str:
    return '\n'.join(map(lambda x: x.strip(), text.split('\n')))


def configure_parser(paser: ArgumentParser) -> ArgumentParser:
    paser.add_argument('file',
                       metavar='FILE',
                       type=str,
                       help='''The parquet file to inspect
                       e.g. ./target.parquet or s3://bucket-name/target.parquet
                       ''')
    paser.add_argument('--awsprofile',
                       type=str,
                       required=False,
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')
    paser.add_argument('--detail',
                       action='store_true',
                       required=False,
                       default=False,
                       help='Detail expression using thrift.')
    paser.add_argument('--endpoint-url',
                       type=str,
                       required=False,
                       default=os.environ.get('ENDPOINT_URL'),
                       help='A custom S3 endpoint URL')

    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    pf: ParquetFile = to_parquet_file(file_exp=args.file, awsprofile=args.awsprofile, endpoint_url=args.endpoint_url)
    if pf.is_wildcard():
        print('Cannot use wildcard for inspection.', file=sys.stderr)
    else:
        try:
            with pf.get_local_path() as local_path:
                if args.detail:
                    _execute_detail(
                        filename=local_path,
                    )
                else:
                    _execute_simple(
                        filename=local_path,
                    )
        except FileNotFoundException as e:
            print(str(e), file=sys.stderr)


def _execute_simple(filename: str) -> None:
    pq_file: pq.ParquetFile = pq.ParquetFile(filename)
    file_meta: pq.FileMetaData = pq_file.metadata
    print(_simple_meta_expression(file_meta))
    file_schema: pq.ParquetSchema = pq_file.schema
    print(_simple_schema_expression(file_meta, file_schema))


def _simple_meta_expression(file_meta: pq.FileMetaData) -> str:
    return dedent(f'''
    ############ file meta data ############
    created_by: {file_meta.created_by}
    num_columns: {file_meta.num_columns}
    num_rows: {file_meta.num_rows}
    num_row_groups: {file_meta.num_row_groups}
    format_version: {file_meta.format_version}
    serialized_size: {file_meta.serialized_size}
    ''')


def _simple_schema_expression(file_meta, schema) -> str:
    columns: List[str] = schema.names
    columns_exp = '\n'.join(columns)

    exp = dedent(f'''
    ############ Columns ############
    {columns_exp}
    ''')
    for i, column in enumerate(columns):
        col = schema.column(i)
        col_meta = file_meta.row_group(0).column(i)
        if col_meta.total_uncompressed_size:
            col_compression_space_saving_ratio = 1 - (col_meta.total_compressed_size / col_meta.total_uncompressed_size)
            col_compression_space_saving_pct = col_compression_space_saving_ratio * 100
            col_compression_space_saving_pct_str = f"{col_compression_space_saving_pct:.0f}%"
        else:
            col_compression_space_saving_pct_str = 'N/A'
        col_compression = f"{col_meta.compression} (space_saved: {col_compression_space_saving_pct_str})"
        exp += dedent(f'''
        ############ Column({column}) ############
        name: {col.name}
        path: {col.path}
        max_definition_level: {col.max_definition_level}
        max_repetition_level: {col.max_repetition_level}
        physical_type: {col.physical_type}
        logical_type: {col.logical_type}
        converted_type (legacy): {col.converted_type}
        compression: {col_compression}
        ''')

    return exp


def _execute_detail(filename: str) -> None:
    print(_obj_to_string(get_filemetadata(filename), sys.stdout.isatty()))


def _obj_to_string(obj, toatty: bool, level: int = 1) -> str:
    color = [
        Fore.RED,
        Fore.YELLOW,
        Fore.GREEN,
        Fore.BLUE
    ]

    extra = ''
    for i in range(level):
        extra += color[i % 4] + '■■■■' + Style.RESET_ALL if toatty else '    '
    ret = ''
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, bytes):
        ret += str(obj)
    else:
        ret += str(obj.__class__.__name__)
        ret += '\n'
        if isinstance(obj, list):
            for e in obj:
                add = extra + _obj_to_string(e, toatty, level + 1)
                if add:
                    ret += add + '\n'
            ret = ret[:-1]
        else:
            for item in obj.__dict__:
                if obj.__dict__[item]:
                    ret += extra
                    ret += str(item) + ' = ' + _obj_to_string(obj.__dict__[item], toatty, level + 1)
                    ret += '\n'
            ret = ret[:-1]
    return ret
