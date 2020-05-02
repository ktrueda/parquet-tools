# parquet-tools for pip


![Run Unittest](https://github.com/ktrueda/parquet-tools/workflows/Run%20Unittest/badge.svg)
![Run CLI test](https://github.com/ktrueda/parquet-tools/workflows/Run%20CLI%20test/badge.svg)

This is pip version of [parquet-tools](https://github.com/apache/parquet-mr).

## Features

- Read Parquet data
- Read Parquet metadata/schema
- Read Parquet file on cloud storage like S3 (Future)

## Installation


```bash
$ pip install git+ssh://github.com/ktrueda/parquet-tools.git
```

## Usage

```bash
$ pq --help
usage: pq [-h] {show,csv,inspect} ...

parquet CLI tools

positional arguments:
  {show,csv,inspect}
    show              Show human readble format. see `show -h`
    csv               Cat csv style. see `csv -h`
    inspect           Inspect parquet file. see `inspect -h`

optional arguments:
  -h, --help          show this help message and exit
```


## Usage Example

#### Show parquet

```bash
$ pq show test.parquet
+-------+-------+---------+
|   one | two   | three   |
|-------+-------+---------|
|  -1   | foo   | True    |
| nan   | bar   | False   |
|   2.5 | baz   | True    |
+-------+-------+---------+
```

#### Cat CSV parquet and [csvq](https://github.com/mithrandie/csvq)

```bash
$ pq csv test.parquet |csvq "select one, three where three"
+-------+-------+
|  one  | three |
+-------+-------+
| -1.0  | True  |
| 2.5   | True  |
+-------+-------+
```


This software includes the work that is distributed in the Apache License 2.0.