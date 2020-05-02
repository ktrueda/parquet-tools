from argparse import ArgumentParser, Namespace
from typing import List

import pyarrow.parquet as pq

from .utils import is_s3_file, fetch_s3_to_tmp, get_aws_session


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
                       default='default',
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')

    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    if is_s3_file(args.file):
        with fetch_s3_to_tmp(aws_session=get_aws_session(args.awsprofile), s3uri=args.file) as localfile:
            _execute(
                filename=localfile,
            )
    else:
        _execute(
            filename=args.file,
        )


def _execute(filename: str) -> None:
    pq_file: pq.ParquetFile = pq.ParquetFile(filename)
    file_meta: pq.FileMetaData = pq_file.metadata
    print(_file_meta_expression(file_meta))
    file_schema: pq.ParquetSchema = pq_file.schema
    print(_schema_expression(file_schema))


def _file_meta_expression(file_meta: pq.FileMetaData) -> str:
    return dedent(f'''
    ############ file meta data ############
    created_by: {file_meta.created_by}
    num_columns: {file_meta.num_columns}
    num_rows: {file_meta.num_rows}
    num_row_groups: {file_meta.num_row_groups}
    format_version: {file_meta.format_version}
    serialized_size: {file_meta.serialized_size}
    ''')


def _schema_expression(schema) -> str:
    columns: List[str] = schema.names
    columns_exp = '\n'.join(columns)

    exp = dedent(f'''
    ############ Columns ############
    {columns_exp}
    ''')
    for i, column in enumerate(columns):
        col = schema.column(i)
        exp += dedent(f'''
        ############ Column({column}) ############
        name: {col.name}
        path: {col.path}
        max_definition_level: {col.max_definition_level}
        max_repetition_level: {col.max_repetition_level}
        physical_type: {col.physical_type}
        logical_type: {col.logical_type}
        converted_type (legacy): {col.converted_type}
        ''')

    return exp
