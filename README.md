# parquet-tools

![Run Unittest](https://github.com/ktrueda/parquet-tools/workflows/Run%20Unittest/badge.svg)
![Run CLI test](https://github.com/ktrueda/parquet-tools/workflows/Run%20CLI%20test/badge.svg)

This is a pip installable [parquet-tools](https://github.com/apache/parquet-mr).
In other words, parquet-tools is a CLI tools of [Apache Arrow](https://github.com/apache/arrow).
You can show parquet file content/schema on local disk or on Amazon S3.
It is incompatible with original parquet-tools.

## Features

- Read Parquet data (local file or file on S3)
- Read Parquet metadata/schema (local file or file on S3)

## Installation

```bash
$ pip install parquet-tools
```

## Usage

```bash
$ parquet-tools --help
usage: parquet-tools [-h] {show,csv,inspect} ...

parquet CLI tools

positional arguments:
  {show,csv,inspect}
    show              Show human readble format. see `show -h`
    csv               Cat csv style. see `csv -h`
    inspect           Inspect parquet file. see `inspect -h`

optional arguments:
  -h, --help          show this help message and exit
```

## Usage Examples

#### Show local parquet file

```bash
$ parquet-tools show test.parquet
+-------+-------+---------+
|   one | two   | three   |
|-------+-------+---------|
|  -1   | foo   | True    |
| nan   | bar   | False   |
|   2.5 | baz   | True    |
+-------+-------+---------+
```

#### Show parquet file on S3

```bash
$ parquet-tools show s3://bucket-name/prefix/*
+-------+-------+---------+
|   one | two   | three   |
|-------+-------+---------|
|  -1   | foo   | True    |
| nan   | bar   | False   |
|   2.5 | baz   | True    |
+-------+-------+---------+
```


#### Inspect parquet file schema

```bash
$ parquet-tools inspect /path/to/parquet
```

<details>

<summary>Inspect output</summary>

```
FileMetaData
■■■■version = 1
■■■■schema = list
■■■■■■■■SchemaElement
■■■■■■■■■■■■name = schema
■■■■■■■■■■■■num_children = 3
■■■■■■■■SchemaElement
■■■■■■■■■■■■type = 5
■■■■■■■■■■■■repetition_type = 1
■■■■■■■■■■■■name = one
■■■■■■■■SchemaElement
■■■■■■■■■■■■type = 6
■■■■■■■■■■■■repetition_type = 1
■■■■■■■■■■■■name = two
■■■■■■■■■■■■logicalType = LogicalType
■■■■■■■■■■■■■■■■STRING = StringType
■■■■■■■■SchemaElement
■■■■■■■■■■■■repetition_type = 1
■■■■■■■■■■■■name = three
■■■■num_rows = 3
■■■■row_groups = list
■■■■■■■■RowGroup
■■■■■■■■■■■■columns = list
■■■■■■■■■■■■■■■■ColumnChunk
■■■■■■■■■■■■■■■■■■■■file_offset = 108
■■■■■■■■■■■■■■■■■■■■meta_data = ColumnMetaData
■■■■■■■■■■■■■■■■■■■■■■■■type = 5
■■■■■■■■■■■■■■■■■■■■■■■■encodings = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■0
■■■■■■■■■■■■■■■■■■■■■■■■■■■■3
■■■■■■■■■■■■■■■■■■■■■■■■path_in_schema = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■one
■■■■■■■■■■■■■■■■■■■■■■■■codec = 1
■■■■■■■■■■■■■■■■■■■■■■■■num_values = 3
■■■■■■■■■■■■■■■■■■■■■■■■total_uncompressed_size = 100
■■■■■■■■■■■■■■■■■■■■■■■■total_compressed_size = 104
■■■■■■■■■■■■■■■■■■■■■■■■data_page_offset = 36
■■■■■■■■■■■■■■■■■■■■■■■■dictionary_page_offset = 4
■■■■■■■■■■■■■■■■■■■■■■■■statistics = Statistics
■■■■■■■■■■■■■■■■■■■■■■■■■■■■max = b'\x00\x00\x00\x00\x00\x00\x04@'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■min = b'\x00\x00\x00\x00\x00\x00\xf0\xbf'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■null_count = 1
■■■■■■■■■■■■■■■■■■■■■■■■■■■■max_value = b'\x00\x00\x00\x00\x00\x00\x04@'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■min_value = b'\x00\x00\x00\x00\x00\x00\xf0\xbf'
■■■■■■■■■■■■■■■■■■■■■■■■encoding_stats = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■PageEncodingStats
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■page_type = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■encoding = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■count = 1
■■■■■■■■■■■■■■■■■■■■■■■■■■■■PageEncodingStats
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■encoding = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■count = 1
■■■■■■■■■■■■■■■■ColumnChunk
■■■■■■■■■■■■■■■■■■■■file_offset = 281
■■■■■■■■■■■■■■■■■■■■meta_data = ColumnMetaData
■■■■■■■■■■■■■■■■■■■■■■■■type = 6
■■■■■■■■■■■■■■■■■■■■■■■■encodings = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■0
■■■■■■■■■■■■■■■■■■■■■■■■■■■■3
■■■■■■■■■■■■■■■■■■■■■■■■path_in_schema = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■two
■■■■■■■■■■■■■■■■■■■■■■■■codec = 1
■■■■■■■■■■■■■■■■■■■■■■■■num_values = 3
■■■■■■■■■■■■■■■■■■■■■■■■total_uncompressed_size = 76
■■■■■■■■■■■■■■■■■■■■■■■■total_compressed_size = 80
■■■■■■■■■■■■■■■■■■■■■■■■data_page_offset = 238
■■■■■■■■■■■■■■■■■■■■■■■■dictionary_page_offset = 201
■■■■■■■■■■■■■■■■■■■■■■■■statistics = Statistics
■■■■■■■■■■■■■■■■■■■■■■■■■■■■max_value = b'foo'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■min_value = b'bar'
■■■■■■■■■■■■■■■■■■■■■■■■encoding_stats = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■PageEncodingStats
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■page_type = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■encoding = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■count = 1
■■■■■■■■■■■■■■■■■■■■■■■■■■■■PageEncodingStats
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■encoding = 2
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■count = 1
■■■■■■■■■■■■■■■■ColumnChunk
■■■■■■■■■■■■■■■■■■■■file_offset = 388
■■■■■■■■■■■■■■■■■■■■meta_data = ColumnMetaData
■■■■■■■■■■■■■■■■■■■■■■■■encodings = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■0
■■■■■■■■■■■■■■■■■■■■■■■■■■■■3
■■■■■■■■■■■■■■■■■■■■■■■■path_in_schema = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■three
■■■■■■■■■■■■■■■■■■■■■■■■codec = 1
■■■■■■■■■■■■■■■■■■■■■■■■num_values = 3
■■■■■■■■■■■■■■■■■■■■■■■■total_uncompressed_size = 40
■■■■■■■■■■■■■■■■■■■■■■■■total_compressed_size = 42
■■■■■■■■■■■■■■■■■■■■■■■■data_page_offset = 346
■■■■■■■■■■■■■■■■■■■■■■■■statistics = Statistics
■■■■■■■■■■■■■■■■■■■■■■■■■■■■max = b'\x01'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■min = b'\x00'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■max_value = b'\x01'
■■■■■■■■■■■■■■■■■■■■■■■■■■■■min_value = b'\x00'
■■■■■■■■■■■■■■■■■■■■■■■■encoding_stats = list
■■■■■■■■■■■■■■■■■■■■■■■■■■■■PageEncodingStats
■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■count = 1
■■■■■■■■■■■■total_byte_size = 226
■■■■■■■■■■■■num_rows = 3
■■■■■■■■■■■■file_offset = 108
■■■■■■■■■■■■total_compressed_size = 226
■■■■key_value_metadata = list
■■■■■■■■KeyValue
■■■■■■■■■■■■key = pandas
■■■■■■■■■■■■value = {"index_columns": [{"kind": "range", "name": null,
■■■■■■■■KeyValue
■■■■■■■■■■■■key = ARROW:schema
■■■■■■■■■■■■value = /////4gDAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABAwAQAAAAAA
■■■■created_by = parquet-cpp version 1.5.1-SNAPSHOT
■■■■column_orders = list
■■■■■■■■ColumnOrder
■■■■■■■■■■■■TYPE_ORDER = TypeDefinedOrder
■■■■■■■■ColumnOrder
■■■■■■■■■■■■TYPE_ORDER = TypeDefinedOrder
■■■■■■■■ColumnOrder
■■■■■■■■■■■■TYPE_ORDER = TypeDefinedOrder
```
</details>

#### Cat CSV parquet and transform [csvq](https://github.com/mithrandie/csvq)

```bash
$ parquet-tools csv s3://bucket-name/test.parquet |csvq "select one, three where three"
+-------+-------+
|  one  | three |
+-------+-------+
| -1.0  | True  |
| 2.5   | True  |
+-------+-------+
```
