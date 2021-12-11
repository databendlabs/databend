---
title: CREATE TABLE
---

Create a new table.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 type1,
    name2 type2,
    ...
) ENGINE = engine
[OPTIONS]
```
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
ENGINE = engine
[OPTIONS]
```
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
ENGINE = engine
[OPTIONS]
AS SELECT query
```

:::note
Local engine is one of `Memory`, `Parquet`, `JSONEachRow`, `Null` or `CSV`, data will be stored in the DatabendQuery memory/disk locally.

Remote engine is `remote`, will be stored in the remote DatabendStore cluster.
:::


## Examples

### Memory engine

```sql
mysql> CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+
```
### Create Table Like statement
```sql
mysql> CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> CREATE TABLE test2 LIKE test Engine = Memory;

mysql> INSERT INTO test2(a,b) values(0, 'sun');

mysql> SELECT * FROM test2;
+------+------+
| a    | b    |
+------+------+
|    0 | sun  |
+------+------+
```

### Create Table As Select (CTAS) statement

```sql
mysql> CREATE TABLE source(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO source(a,b) values(888, 'stars');

mysql> SELECT * FROM source;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> CREATE TABLE copy1 AS SELECT * FROM source;

mysql> SELECT * FROM copy1;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
+------+-------+

mysql> CREATE TABLE copy2(x VARCHAR, y VARCHAR) AS SELECT * FROM source;

mysql> SELECT * FROM copy2;
+------+-------+
| x    | y     |
+------+-------+
| 888  | stars |
+------+-------+
```