from os import path
from os.path import dirname

from subprocess import check_output

from parquet_tools.parquet.reader import get_filemetadata
from parquet_tools.gen_py.parquet.ttypes import (FileMetaData, SchemaElement, LogicalType, StringType, RowGroup, ColumnMetaData, ColumnChunk,
                                                 Statistics, PageEncodingStats, KeyValue)
import pytest


class TestGetMetaData:
    @pytest.fixture
    def fmd(self) -> FileMetaData:
        fmd: FileMetaData = get_filemetadata("./tests/test1.parquet")
        return fmd

    def test_version(self, fmd):
        assert fmd.version == 2

    def test_schma(self, fmd):
        assert fmd.schema == [
            SchemaElement(
                type=None,
                type_length=None,
                repetition_type=0,
                name='schema',
                num_children=3,
                converted_type=None,
                scale=None,
                precision=None,
                field_id=None,
                logicalType=None),
            SchemaElement(
                type=5,
                type_length=None,
                repetition_type=1,
                name='one',
                num_children=None,
                converted_type=None,
                scale=None,
                precision=None,
                field_id=None,
                logicalType=None),
            SchemaElement(
                type=6,
                type_length=None,
                repetition_type=1,
                name='two',
                num_children=None,
                converted_type=0,
                scale=None,
                precision=None,
                field_id=None,
                logicalType=LogicalType(
                    STRING=StringType(),
                    MAP=None,
                    LIST=None,
                    ENUM=None,
                    DECIMAL=None,
                    DATE=None,
                    TIME=None,
                    TIMESTAMP=None,
                    INTEGER=None,
                    UNKNOWN=None,
                    JSON=None,
                    BSON=None,
                    UUID=None)
            ),
            SchemaElement(
                type=0,
                type_length=None,
                repetition_type=1,
                name='three',
                num_children=None,
                converted_type=None,
                scale=None,
                precision=None,
                field_id=None,
                logicalType=None)
        ]

    def test_num_rows(self, fmd):
        assert fmd.num_rows == 3

    def test_row_groups(self, fmd):
        assert fmd.row_groups == [
            RowGroup(
                columns=[
                    ColumnChunk(
                        file_path=None,
                        file_offset=108,
                        meta_data=ColumnMetaData(
                            type=5,
                            encodings=[0, 3, 8],
                            path_in_schema=['one'],
                            codec=1,
                            num_values=3,
                            total_uncompressed_size=100,
                            total_compressed_size=104,
                            key_value_metadata=None,
                            data_page_offset=36,
                            index_page_offset=None,
                            dictionary_page_offset=4,
                            statistics=Statistics(
                                max=b'\x00\x00\x00\x00\x00\x00\x04@',
                                min=b'\x00\x00\x00\x00\x00\x00\xf0\xbf',
                                null_count=1,
                                distinct_count=None,
                                max_value=b'\x00\x00\x00\x00\x00\x00\x04@',
                                min_value=b'\x00\x00\x00\x00\x00\x00\xf0\xbf'
                            ),
                            encoding_stats=[
                                PageEncodingStats(
                                    page_type=2,
                                    encoding=0,
                                    count=1),
                                PageEncodingStats(
                                    page_type=0,
                                    encoding=8,
                                    count=1)],
                            bloom_filter_offset=None),
                        offset_index_offset=None,
                        offset_index_length=None,
                        column_index_offset=None,
                        column_index_length=None,
                        crypto_metadata=None,
                        encrypted_column_metadata=None),
                    ColumnChunk(
                        file_path=None,
                        file_offset=281,
                        meta_data=ColumnMetaData(
                            type=6,
                            encodings=[0, 3, 8],
                            path_in_schema=['two'],
                            codec=1,
                            num_values=3,
                            total_uncompressed_size=76,
                            total_compressed_size=80,
                            key_value_metadata=None,
                            data_page_offset=238,
                            index_page_offset=None,
                            dictionary_page_offset=201,
                            statistics=Statistics(
                                max=None,
                                min=None,
                                null_count=0,
                                distinct_count=None,
                                max_value=b'foo',
                                min_value=b'bar'),
                            encoding_stats=[
                                PageEncodingStats(
                                    page_type=2,
                                    encoding=0,
                                    count=1),
                                PageEncodingStats(
                                    page_type=0,
                                    encoding=8,
                                    count=1)
                            ],
                            bloom_filter_offset=None
                        ),
                        offset_index_offset=None,
                        offset_index_length=None,
                        column_index_offset=None,
                        column_index_length=None,
                        crypto_metadata=None,
                        encrypted_column_metadata=None
                    ),
                    ColumnChunk(
                        file_path=None,
                        file_offset=388,
                        meta_data=ColumnMetaData(
                            type=0,
                            encodings=[3, 0],
                            path_in_schema=['three'],
                            codec=1,
                            num_values=3,
                            total_uncompressed_size=40,
                            total_compressed_size=42,
                            key_value_metadata=None,
                            data_page_offset=346,
                            index_page_offset=None,
                            dictionary_page_offset=None,
                            statistics=Statistics(
                                max=b'\x01',
                                min=b'\x00',
                                null_count=0,
                                distinct_count=None,
                                max_value=b'\x01',
                                min_value=b'\x00'),
                            encoding_stats=[
                                PageEncodingStats(
                                    page_type=0,
                                    encoding=0,
                                    count=1)
                            ],
                            bloom_filter_offset=None
                        ),
                        offset_index_offset=None,
                        offset_index_length=None,
                        column_index_offset=None,
                        column_index_length=None,
                        crypto_metadata=None,
                        encrypted_column_metadata=None
                    )],
                total_byte_size=216,
                num_rows=3,
                sorting_columns=None,
                file_offset=4,
                total_compressed_size=226,
                ordinal=0)
        ]

    def test_key_value_metadata(self, fmd):
        assert fmd.key_value_metadata == [
            KeyValue(
                key='pandas',
                value='{"index_columns": [{"kind": "range", "name": null, "start": 0, "stop": 3, "step": 1}], "column_indexes": [{"name": null, "field_name": null, "pandas_type": "unicode", "numpy_type": "object", "metadata": {"encoding": "UTF-8"}}], "columns": [{"name": "one", "field_name": "one", "pandas_type": "float64", "numpy_type": "float64", "metadata": null}, {"name": "two", "field_name": "two", "pandas_type": "unicode", "numpy_type": "object", "metadata": null}, {"name": "three", "field_name": "three", "pandas_type": "bool", "numpy_type": "bool", "metadata": null}], "creator": {"library": "pyarrow", "version": "14.0.2"}, "pandas_version": "2.1.4"}'),
            KeyValue(
                key='ARROW:schema',
                value='/////4gDAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAALgCAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAACQAgAABAAAAIECAAB7ImluZGV4X2NvbHVtbnMiOiBbeyJraW5kIjogInJhbmdlIiwgIm5hbWUiOiBudWxsLCAic3RhcnQiOiAwLCAic3RvcCI6IDMsICJzdGVwIjogMX1dLCAiY29sdW1uX2luZGV4ZXMiOiBbeyJuYW1lIjogbnVsbCwgImZpZWxkX25hbWUiOiBudWxsLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IHsiZW5jb2RpbmciOiAiVVRGLTgifX1dLCAiY29sdW1ucyI6IFt7Im5hbWUiOiAib25lIiwgImZpZWxkX25hbWUiOiAib25lIiwgInBhbmRhc190eXBlIjogImZsb2F0NjQiLCAibnVtcHlfdHlwZSI6ICJmbG9hdDY0IiwgIm1ldGFkYXRhIjogbnVsbH0sIHsibmFtZSI6ICJ0d28iLCAiZmllbGRfbmFtZSI6ICJ0d28iLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IG51bGx9LCB7Im5hbWUiOiAidGhyZWUiLCAiZmllbGRfbmFtZSI6ICJ0aHJlZSIsICJwYW5kYXNfdHlwZSI6ICJib29sIiwgIm51bXB5X3R5cGUiOiAiYm9vbCIsICJtZXRhZGF0YSI6IG51bGx9XSwgImNyZWF0b3IiOiB7ImxpYnJhcnkiOiAicHlhcnJvdyIsICJ2ZXJzaW9uIjogIjE0LjAuMiJ9LCAicGFuZGFzX3ZlcnNpb24iOiAiMi4xLjQifQAAAAYAAABwYW5kYXMAAAMAAABsAAAAMAAAAAQAAACw////AAABBhAAAAAYAAAABAAAAAAAAAAFAAAAdGhyZWUAAADc////2P///wAAAQUQAAAAGAAAAAQAAAAAAAAAAwAAAHR3bwAEAAQABAAAABAAFAAIAAYABwAMAAAAEAAQAAAAAAABAxAAAAAcAAAABAAAAAAAAAADAAAAb25lAAAABgAIAAYABgAAAAAAAgAAAAAA')
        ]

    def test_inspect(self):
        tests_dir = dirname(__file__)
        test0_parquet_path = path.join(tests_dir, 'test0.parquet')
        actual = check_output(['parquet-tools', 'inspect', test0_parquet_path]).decode()
        expected_path = path.join(tests_dir, 'test0_inspect.txt')
        with open(expected_path, 'r') as f:
            expected = f.read()
        assert actual == expected
