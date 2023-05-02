---
title: BendSQL
sidebar_label: BendSQL
description:
  Databend-native CLI
---

[BendSQL](https://github.com/datafuselabs/BendSQL) is a command line tool that has been designed specifically for Databend. It allows users to establish a connection with Databend and execute queries directly from a CLI window.

This tool is particularly useful for those who prefer a command line interface and need to work with Databend on a regular basis. With BendSQL, users can easily and efficiently manage their databases, tables, and data, and perform a wide range of queries and operations with ease.

**Related video:**

<iframe width="853" height="505" className="iframe-video" src="https://www.youtube.com/embed/3cFmGvtU-ws" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## Downloading and Installing BendSQL

To download and install BendSQL, kindly visit the [BendSQL release page](https://github.com/datafuselabs/BendSQL/releases) on GitHub.

## Connecting to Databend

Use `bendsql` to connect to a Databend instance:

```shell
> bendsql --help
Databend Native Command Line Tool

Usage: bendsql [OPTIONS]

Options:
      --help                     Print help information
      --flight                   Using flight sql protocol
      --tls                      Enable TLS
  -h, --host <HOST>              Databend Server host, Default: 127.0.0.1
  -P, --port <PORT>              Databend Server port, Default: 8000
  -u, --user <USER>              Default: root
  -p, --password <PASSWORD>      [env: BENDSQL_PASSWORD=]
  -D, --database <DATABASE>      Database name
      --set <SET>                Settings
      --dsn <DSN>                Data source name [env: BENDSQL_DSN=]
  -n, --non-interactive          Force non-interactive mode
  -q, --query <QUERY>            Query to execute
  -d, --data <DATA>              Data to load, @file or @- for stdin
  -f, --format <FORMAT>          Data format to load [default: csv] [possible values: csv, tsv, ndjson, parquet, xml]
      --format-opt <FORMAT_OPT>  Data format options
  -o, --output <OUTPUT>          Output format [default: table] [possible values: table, csv, tsv]
      --progress                 Show progress for data loading in stderr
  -V, --version                  Print version
```

To connect to a local Databend, simply run `bendsql`:

```shell
> bendsql
Welcome to BendSQL.
Trying connect to localhost:8000 as user root.
Connected to DatabendQuery v1.1.2-nightly-8ade21e4669e0a2cc100615247705feacdf76c5b(rust-1.70.0-nightly-2023-04-15T16:08:52.195357424Z)
```

To connect to Databend Cloud, it is recommended to use the `--dsn` option or the `BENDSQL_DSN` environment variable:

```shell
> export BENDSQL_DSN="databend://cloudapp:password@tnxxx.gw.aws-us-east-2.default.databend.com/?warehouse=default

> bendsql
Welcome to BendSQL.
Trying connect to tnxxx.gw.aws-us-east-2.default.datafusecloud.com:443 as user cloudapp.
Connected to DatabendQuery v1.1.17-nightly-77286d52c6d6db2c2000a74febf4ddb25f910c41(rust-1.70.0-nightly-2023-04-24T04:38:16.901421116Z)

cloudapp@tnxxx.gw>
```

## Running Queries with BendSQL

### Query with interactive shell

```shell
> bendsql
Welcome to BendSQL.
Trying connect to localhost:8000 as user root.
Connected to DatabendQuery v1.1.2-nightly-8ade21e4669e0a2cc100615247705feacdf76c5b(rust-1.70.0-nightly-2023-04-15T16:08:52.195357424Z)

root@localhost> select now();

SELECT
  NOW()

┌────────────────────────────┐
│            now()           │
│          Timestamp         │
├────────────────────────────┤
│ 2023-04-25 07:09:41.281690 │
└────────────────────────────┘

1 row in 0.018 sec. Processed 1 rows, 1B (54.96 rows/s, 54B/s)
(1 row)
```

### Query non-interactive

with argument:
```shell
> bendsql --query "select now()"
┌────────────────────────────┐
│            now()           │
│          Timestamp         │
├────────────────────────────┤
│ 2023-04-25 07:12:31.552631 │
└────────────────────────────┘
```

with stdin:
```shell
> echo "select now();" | bendsql
┌────────────────────────────┐
│            now()           │
│          Timestamp         │
├────────────────────────────┤
│ 2023-04-25 07:12:55.602754 │
└────────────────────────────┘
```

## Loading data

from stdin:
```shell
> bendsql --query='INSERT INTO test_books VALUES;' --format=csv --data=@- <books.csv
```

from file:
```shell
> bendsql \
    --query='INSERT INTO ontime VALUES;' \
    --format=csv \
    --format-opt="compression=gzip" \
    --format-opt="skip_header=1" \
    --set="presigned_url_disabled=1" \
    --data=@ontime.csv.gz
```

:::note
`presigned_url_disabled=1` would instruct BendSQL to load data directly using `upload_to_stage` api, which would result in additional transfer fee as well as lower performance, and is not recommended for production use.
:::