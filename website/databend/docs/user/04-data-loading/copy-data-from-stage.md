---
title: Copy Data From Stage
draft: true
---

Copy data from stage.

## Copy into Statement
### Syntax

```
copy into default.test_csv from '@s3_stage/tests/data/sample.csv' format CSV field_delimitor = ',';

COPY INTO [<db>.]<table_name> [ <schema> ]
    FROM { stage_location }
    FORMAT <format_name>
    [options]
```

### Parameters

  * `db`: database name
  * `table_name`: table name
  * `schema`: optional schema fields, eg:  `(a,b,c)`
  * `stage_location`: stage location, eg:  `@s3_stage/tests/data/sample.csv`
  * `format_name`: format name, supported format:  `CSV`, `Parquet`
  * `options`: other options, supported options:  `field_delimitor`, `record_delimitor`, `csv_header`


### Examples

#### COPY from csv file

Example:
```sql
mysql> create table default.test_csv (id int,name varchar(255),rank int);

mysql> copy into default.test_csv from '@s3_stage/tests/data/sample.csv' format CSV field_delimitor = ',';
Query OK, 0 rows affected (0.17 sec)
Read 6 rows, 163 B in 0.160 sec., 37.53 rows/sec., 1.02 KB/sec.

mysql> select max(id), min(name), avg(rank)  from default.test_csv;

+---------+-----------+-------------------+
| max(id) | min(name) | avg(rank)         |
+---------+-----------+-------------------+
|       6 | 'Beijing' | 77.33333333333333 |
+---------+-----------+-------------------+
1 row in set (0.13 sec)
Read 6 rows, 163 B in 0.042 sec., 143.43 rows/sec., 3.9 KB/sec.
```
