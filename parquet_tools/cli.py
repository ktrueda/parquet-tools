import argparse
import os
import sys

from parquet_tools.commands import show, csv, inspect


def main() -> None:
    parser = argparse.ArgumentParser(
        prog='parquet-tools',
        description='parquet CLI tools'
    )
    subparsers = parser.add_subparsers()
    show.configure_parser(subparsers.add_parser(
        'show',
        help='Show human readable format. see `show -h`',
        description='Show parquet file content with human readability.'))
    csv.configure_parser(subparsers.add_parser(
        'csv',
        help='Cat csv style. see `csv -h`',
        description='Cat parquet as csv style.'))
    inspect.configure_parser(subparsers.add_parser(
        'inspect',
        help='Inspect parquet file. see `inspect -h`',
        description='Inspect parquet file.'))

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        try:
            args.handler(args)
        except BrokenPipeError:
            # Python flushes standard streams on exit; redirect remaining output
            # to devnull to avoid another BrokenPipeError at shutdown
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            sys.exit(1)  # Python exits with error code 1 on EPIPE
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
