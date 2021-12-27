---
title: INSERT
---

Writing data.

## Insert Into Statement
### Syntax

```sql
INSERT INTO|OVERWRITE [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```


:::note
Local engine is one of `Memory`, `Parquet`, `JSONEachRow`, `Null` or `CSV`, data will be stored in the DatabendQuery memory/disk locally.

Remote engine is `remote`, will be stored in the remote DatabendStore cluster.
:::

### Examples

#### Memory engine

Example:
```sql
mysql> CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO test(a,b) values(888, 'stars');
mysql> INSERT INTO test values(1024, 'stars');

mysql> SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
| 1024 | stars |
+------+-------+

mysql> INSERT OVERWRITE test values(2048, 'stars');
mysql> SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
| 2048 | stars |
+------+-------+
```

## Inserting the Results of SELECT
### Syntax

```
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

:::note
Columns are mapped according to their position in the SELECT clause, So the number of columns in SELECT should be greater or equal to the INSERT table.

The data type of columns in the SELECT and INSERT table could be different, if necessary, type casting will be performed. 
:::

### Examples

#### Memory engine

Example:
```sql
mysql> CREATE TABLE select_table(a Varchar, b Varchar, c Varchar) Engine = Memory;
mysql> INSERT INTO select_table values('1','11','abc');
mysql> SELECT * FROM select_table;
+------+------+------+
| a    | b    | c    |
+------+------+------+
| 1    | 11   | abc  |
+------+------+------+

mysql> CREATE TABLE test(c1 UInt8, c2 UInt64, c3 String) Engine = Memory;
mysql> INSERT INTO test SELECT * FROM select_table;
mysql> SELECT * from test;
+------+------+------+
| c1   | c2   | c3   |
+------+------+------+
|    1 |   11 | abc  |
+------+------+------+

mysql> SELECT toTypeName(c1), toTypeName(c2), toTypeName(c3) from test;
+----------------+----------------+----------------+
| toTypeName(c1) | toTypeName(c2) | toTypeName(c3) |
+----------------+----------------+----------------+
| UInt8          | UInt64         | String         |
+----------------+----------------+----------------+
```

Aggregate Example:
```sql
# create table
mysql> CREATE TABLE base_table(a Int32);
mysql> CREATE TABLE aggregate_table(b Int32);

# insert some datas to base_table
mysql> INSERT INTO base_table VALUES(1),(2),(3),(4),(5),(6);

# insert into aggregate_table from the aggregation
mysql> INSERT INTO aggregate_table SELECT SUM(a) FROM base_table GROUP BY a%3;

mysql> SELECT * FROM aggregate_table ORDER BY b;
+------+
| b    |
+------+
|    5 |
|    7 |
|    9 |
+------+
```
