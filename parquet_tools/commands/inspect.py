import sys
from argparse import ArgumentParser, Namespace

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
                       default='default',
                       help='awscli profile in ~/.aws/credentials. You use this option when you read parquet file on s3.')

    paser.set_defaults(handler=_cli)
    return paser


def _cli(args: Namespace) -> None:
    pf: ParquetFile = to_parquet_file(file_exp=args.file, awsprofile=args.awsprofile)
    if pf.is_wildcard():
        print('Cannot use wildcard for inspection.', file=sys.stderr)
    else:
        try:
            with pf.get_local_path() as local_path:
                _execute(
                    filename=local_path,
                )
        except FileNotFoundException as e:
            print(str(e), file=sys.stderr)


def _execute(filename: str) -> None:
    print(_obj_to_string(get_filemetadata(filename)))


def _obj_to_string(obj, level: int = 1) -> str:
    color = [
        Fore.RED,
        Fore.YELLOW,
        Fore.GREEN,
        Fore.BLUE
    ]

    extra = ''
    for i in range(level):
        extra += color[i % 4] + '■■■■' + Style.RESET_ALL
    ret = ''
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, bytes):
        ret += str(obj)[:50]
    else:
        ret += str(obj.__class__.__name__)
        ret += '\n'
        if isinstance(obj, list):
            for e in obj:
                add = extra + _obj_to_string(e, level + 1)
                if add:
                    ret += add + '\n'
            ret = ret[:-1]
        else:
            for item in obj.__dict__:
                if obj.__dict__[item]:
                    ret += extra
                    ret += str(item) + ' = ' + _obj_to_string(obj.__dict__[item], level + 1)
                    ret += '\n'
            ret = ret[:-1]
    return ret
