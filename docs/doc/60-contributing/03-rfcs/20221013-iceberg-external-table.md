---
title: Iceberg External Table
description: Accessing Apache Iceberg formatted data source as external table.
---

- RFC PR: [datafuselabs/databend#8215](https://github.com/datafuselabs/databend/pull/8215)
- Tracking Issue: [datafuselabs/databend#8216](https://github.com/datafuselabs/databend/issues/8216)

## Summary

Iceberg is a widely supported table format among data lake-houses.
This RFC describes how the iceberg external table will behave and how we will implement it.

## Motivation

The [Apache Iceberg](https://iceberg.apache.org) is a table format widely used in data lake-houses, it offers a more complete logical view of databases and has much higher performance of accessing as external tables, since users don't need to know about partitions and less indexing through files when accessing is required comparing to `Hive`.

The splitted and well-defined structure of `Iceberg` also makes concurrent accesssing and version managementing on data sources safer, saver and convienient.

![stack of Apache Iceberg](https://iceberg.apache.org/img/iceberg-metadata.png)

Supporting Iceberg will empower databend as a open data lake, giving it more OLAP abilities.

## Guide-level explanation

### Create Iceberg Table

To use Iceberg external table, users need to have an iceberg storage with neccessary permission established. Then they can establish a external table with `ICEBERG` engine:

```sql
CREATE EXTERNAL TABLE [IF NOT EXISTS] [db.]table_name
[(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
)] ENGINE=ICEBERG
ENGINE_OPTIONS=(
  DATABASE='db0'
  TABLE='tbl0'
  LOCATION=<external-location>
)
```

where:

- data_type:

```
<data_type>:
  TINYINT
| SMALLINT
| INT
| BIGINT
| FLOAT
| DOUBLE
| DATE
| TIMESTAMP
| VARCHAR
| ARRAY
| OBJECT
| VARIANT
```

- external-location (using S3 for example):

```
external-location ::=

URL = 's3://<bucket-name>/<path-to-iceberg>'
CONNECTION = (
  ENDPOINT_URL = <endpoint_url>
  ACCESS_KEY_ID = <access_key_id>
  SECRET_ACCESS_KEY = <secret_access_key>
  SESSION_TOKEN = <aws_session_token>
)
```

- ENGINE_OPTIONS

```
ENGINE_OPTIONS=(
  DATABASE=<database-name>
  TABLE=<table-name>
  [SNAPSHOT= { snapshot_id => <snapshot-id> | timestamp => <timestamp> }]
)
```

For example:

```sql
CREATE EXTERNAL TABLE icebergs.books (
    author VARCHAR,
    name VARCHAR,
    date VARCHAR,
) ENGINE=ICEBERG
ENGINE_OPTIONS=(
    DATABASE='shared_to_databend'
    TABLE='books'
    LOCATION=(
        URL='s3://example/path/to/iceberg/'
        CONNECTION=(
            ENDPOINT_URL='https://s3.minio.local'
            ACCESS_KEY_ID='example-id'
            SECRET_ACCESS_KEY='example-key'
        )
    )
)
```

For convenience, this will create the same schema as the table inside the external Iceberg storage.

```sql

CREATE EXTERNAL TABLE [IF [NOT] EXISTS] [db.]<table_name>
ENGINE=ICEBERG
ENGINE_OPTIONS=(
    DATABASE=<database-name>
    TABLE=<table-name>
    LOCATION=...
)
```

### Accessing Iceberg Table's content

```sql
-- assuming users have created an external table named `iceberg_tbl`
SELECT COUNT(*) FROM iceberg_tbl;
```

Joint query on normal table and Iceberg Table:

```sql
SELECT normal_tbl.book_name, iceberg_tbl.author FROM normal_tbl, iceberg_tbl WHERE normal_tbl.isbn = iceberg_tbl.isbn AND iceberg_tbl.sales > 100000;
```

On operating the table, all data remains still on the user-provided ends.

### Time Travel

Iceberg offers a list of snapshots and its timestamps. Time travelling on it is natrually.

```sql
SELECT ...
FROM iceberg_table
AT ( { SNAPSHOT => <snapshot_id> | TIMESTAMP => <timestamp> } );
```

#### Accessing Snapshot Metadata

Users should be able to lookup the list of snapshot id in Iceberg:

```sql
SELECT snapshot_id FROM ICEBERG_SNAPSHOT('<database-name>', '<external-iceberg-table-name>');
```

```
     snapshot_id
---------------------
 1234567890123456789
 1234567890123456790
(2 rows)
```

Current snapshot id the external table is reading will always be the newest. But users can using time travel with `AT`.

## Reference-level explanation

A new table engine `ICEBERG`, and options like `ENGINE_OPTIONS` for engine configuration and `external-location` in DDL will be added.

### `ICEBERG` Engine

`ICEBERG` is a table engine that enable users reading data from established Apache Iceberg endpoints. All table content and metadata of external table should remain in user-provided Iceberg data sources, in Iceberg's manner.

The engine will track the last commited snapshot, and should able to read from former snapshots.

### ENGINE_OPTIONS

Apache Iceberg has its own defined database and tables, but this external table only concerned about single tables in the Iceberg storage. Using ENGINE_OPTIONS users can specify which table in which database should the external access from Iceberg.

### external-location

No matter where the Iceberg is, like S3, GCS or OSS, if Databend support the storage, then the Iceberg should be accessable. Users need to tell databend where the Iceberg storage is and how should databend access the storage.

### Type convention

| Iceberg         | Note                                                     | Databend                             |
| --------------- | -------------------------------------------------------- | ------------------------------------ |
| `boolean`       | True or false                                            | `BOOLEAN`                            |
| `int`           | 32 bit signed integer                                    | `INT32`                              |
| `long`          | 64 bit signed integer                                    | `INT64`                              |
| `float`         | 32 bit IEEE 754 float point number                       | `FLOAT`                              |
| `double`        | 64 bit IEEE 754 float point number                       | `DOUBLE`                             |
| `decimal(P, S)` | Fixed point decimal; precision P, scale S                | not supported                        |
| `date`          | Calendar date without timezone or time                   | `DATE`                               |
| `time`          | Timestamp without date, timezone                         | `TIMESTAMP`, convert date to today   |
| `timestamp`     | Timestamp without timezone                               | `TIMESTAMP`, convert timezone to GMT |
| `timestamptz`   | Timestamp with timezone                                  | `TIMESTAMP`                          |
| `string`        | UTF-8 string                                             | `VARCHAR`                            |
| `uuid`          | 16-byte fixed byte array, universally unique identifiers | `VARCHAR`                            |
| `fixed(L)`      | Fixed-length byte array of length L                      | `VARCHAR`                            |
| `binary`        | Arbitrary-length byte array                              | `VARCHAR`                            |
| `struct`        | a tuple of typed values                                  | `OBJECT`                             |
| `list`          | a collection of values with some element type            | `ARRAY`                              |
| `map`           |                                                          | `OBJECT`                             |

## Drawbacks

None

## Rationale and alternatives

### Create Table from Iceberg Snapshots

Iceberg provides a snapshot capability and tables can be created from them.

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
) ICEBERG_SNAPSHOT_LOCATION=
<external-location>
```

The reason why this is not chosen is that this may require copying data from Iceberg into our storage. This may not be expected by users, since almost all data lakes keep the Iceberg data unmoved, and this way is harder to sync up with Iceberg data source.

If users do want to copy data out of Iceberg, this might be done with a `COPY INTO` option:

```sql
COPY INTO [db.]table_name
<file-format>
[<iceberg-options>]
```

where:

```
iceberg-options ::=

ICEBERG_OPTIONS = (
DATABASE=<database-name>
TABLE=<table-name>
LOCATION=<external-location>
[SNAPSHOT=<snapshot-id>]
)
```

## Prior art

None

## Unresolved questions

None

## Future possibilities

### Schema Evolution

The default Iceberg snapshot use in external table should always be the newest commited one:

```sql
SELECT snapshot_id from ICEBERG_SNAPSHOT('example_db', 'iceberg_tbl');
```

```
  snapshot_id
---------------
 0000000000001
```

Supporting schema evolution making databend able to modify contents of Iceberg.
For example, inserting a new record into Iceberg Table:

```sql
INSERT INTO example_db.iceberg_tbl VALUES ('datafuselabs', 'How To Be a Healthy DBA', '2022-10-14');
```

This will create a new snapshot in Iceberg Storage:

```sql
SELECT snapshot_id from ICEBERG_SNAPSHOT('example_db', 'iceberg_tbl');
```

```
  snapshot_id
---------------
 0000000000001
 0000000000002
```
