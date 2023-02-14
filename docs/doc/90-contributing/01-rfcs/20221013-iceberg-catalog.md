---
title: Iceberg Catalog
description: Accessing Apache Iceberg formatted data source as an external catalog.
---

- RFC PR: [datafuselabs/databend#8215](https://github.com/datafuselabs/databend/pull/8215)
- Tracking Issue: [datafuselabs/databend#8216](https://github.com/datafuselabs/databend/issues/8216)

## Summary

Iceberg is a widely supported table format among data lake-houses.
This RFC describes how the iceberg external catalog will behave and how we will implement it.

## Motivation

The [Apache Iceberg](https://iceberg.apache.org) is a table format widely used in data lake-houses, it offers a more complete logical view of databases and has much higher performance of accessing as external tables, since users don't need to know about partitions and less indexing through files when accessing is required comparing to `Hive`.

The split and well-defined structure of `Iceberg` also makes concurrent accesssing and version managementing on data sources safer, saver and convenient.

![stack of Apache Iceberg](https://iceberg.apache.org/img/iceberg-metadata.png)

Supporting Iceberg will empower databend as a open data lake, giving it more OLAP abilities.

## Guide-level explanation

### Create Iceberg Catalog

To use Iceberg catalog, users need to have an iceberg storage with necessary permission established. Then they can create a `catalog` on it.

```sql
CREATE CATALOG my_iceberg
  TYPE="iceberg"
  URL='s3://path/to/iceberg'
  CONNECTION=(
    ACCESS_KEY_ID=...
    SECRET_ACCESS_KEY=...
    ...
  )
```

### Accessing Iceberg Table's content

```sql
SELECT * FROM my_iceberg.iceberg_db.iceberg_tbl;
```

Joint query on normal table and Iceberg Table:

```sql
SELECT normal_tbl.book_name, my_iceberg.iceberg_db.iceberg_tbl.author FROM normal_tbl, iceberg_tbl WHERE normal_tbl.isbn = my_iceberg.iceberg_db.iceberg_tbl.isbn AND iceberg_tbl.sales > 100000;
```

On operating the table, all data remains still on the user-provided ends.

### Time Travel

Iceberg offers a list of snapshots and its timestamps. Time travelling on it is naturally.

```sql
SELECT ...
FROM <iceberg_catalog>.<database_name>.<table_name>
AT ( { SNAPSHOT => <snapshot_id> | TIMESTAMP => <timestamp> } );
```

#### Accessing Snapshot Metadata

Users should be able to lookup the list of snapshot id in catalog:

```sql
SELECT snapshot_id FROM ICEBERG_SNAPSHOT(my_iceberg.iceberg_db.iceberg_tbl);
```

```
     snapshot_id
---------------------
 1234567890123456789
 1234567890123456790
(2 rows)
```

Current snapshot id the external table is reading will always be the newest. But users can using `time travel` with `AT`.

## Reference-level explanation

A new catalog type `ICEBERG`, and table engine for reading data from Iceberg storage.

### Table Engine

The table engine enables users reading data from established Apache Iceberg endpoints. All table content and metadata of external table should remain in user-provided Iceberg data sources, in Iceberg's manner.

The engine will track the last committed snapshot, and should able to read from former snapshots.

### external-location

No matter where the Iceberg is, like S3, GCS or OSS, if Databend support the storage, then the Iceberg should be accessible. Users need to tell databend where the Iceberg storage is and how should databend access the storage.

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

### Iceberg External Table

Creating an external table from Iceberg storage:

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

A new table engine `ICEBERG` will be introduced, and all data will remain still in the Iceberg storage. The external table also should support users to query its snapshot data and time travelling.

```sql
SELECT snapshot_id FROM ICEBERG_SNAPSHOT('<db_name>', '<external_table_name');
```

```
 snapshot_id
--------------
73556087355608
```

After discussion, the catalog way above is chosen, for its more complete support to Iceberg features, and it is more aligned to the current design of Hive catalog.

## Prior art

None

## Unresolved questions

None

## Future possibilities

### Create Table from Iceberg Snapshots

Iceberg provides a snapshot capability and tables can be created from them.

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
) ENGINE = `ICEBERG`
ENGINE_OPTIONS = (
URL = 's3://path/to/iceberg'
DATABASE = <iceberg_db>
TABLE = <iceberg_tbl>
[ SNAPSHOT = { SNAPSHOT_ID => <snapshot_id> | TIMESTAMP => <timestamp> } ]
)
```

### Schema Evolution

The default Iceberg snapshot use in external table should always be the newest committed one:

```sql
SELECT snapshot_id from ICEBERG_SNAPSHOT(iceberg_catalog.iceberg_db.iceberg_tbl);
```

```
  snapshot_id
---------------
 0000000000001
```

Supporting schema evolution making databend able to modify contents of Iceberg.
For example, inserting a new record into Iceberg Table:

```sql
INSERT INTO iceberg_catalog.iceberg_db.iceberg_tbl VALUES ('datafuselabs', 'How To Be a Healthy DBA', '2022-10-14');
```

This will create a new snapshot in Iceberg Storage:

```sql
SELECT snapshot_id from ICEBERG_SNAPSHOT(iceberg_catalog.iceberg_db.iceberg_tbl);
```

```
  snapshot_id
---------------
 0000000000001
 0000000000002
```
