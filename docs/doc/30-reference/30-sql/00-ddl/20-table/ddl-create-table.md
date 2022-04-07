---
title: CREATE TABLE
---

Create a new table.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }],
    ...
)

<data_type>:
  Int8
| UInt8
| Int16
| UInt16
| Int32
| UInt32
| Int64
| UInt64
| Float32
| Float64
| Date
| Date32
| DateTime
| DateTime64
| String
| Variant
```

:::tip
Data type reference:
* [Integer Numbers Data Type](../../../10-data-types/data-type-integer-number.md)
* [Real Numbers Data Type](../../../10-data-types/data-type-real-number.md)
* [Time and Date Data Type](../../../10-data-types/data-type-time-date-types.md)
* [String Data Type](../../../10-data-types/data-type-string-types.md)
* [Semi-structured Data Type](../../../10-data-types/data-type-semi-structured-types.md)
:::


```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
```
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
AS SELECT query
```

## Column Nullable

By default, **all columns are not nullable(NOT NULL)**, if you want to special a column default to `NULL`, please use:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> NULL,
     ...
)
```

```sql
create table t1(a Int32 NULL);
```

## Default Values
```sql
DEFAULT <expression>
```
Specifies a default value inserted in the column if a value is not specified via an INSERT or CREATE TABLE AS SELECT statement.

## MySQL Compatibility

Databend’s syntax is difference from MySQL mainly in the data type and some specific index hints.

## Examples

```sql
mysql> CREATE TABLE test(a UInt64, b Varchar, c Varchar DEFAULT concat(b, '-b'));

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> select * from test;
+------+-------+---------+
| a    | b     | c       |
+------+-------+---------+
|  888 | stars | stars-b |
+------+-------+---------+
```
### Create Table Like statement
```sql
mysql> CREATE TABLE test(a UInt64, b Varchar);

mysql> INSERT INTO test(a,b) values(888, 'stars');

mysql> SELECT * FROM test;
+------+---------+
| a    | b       |
+------+---------+
|  888 |  stars  |
+------+---------+

mysql> CREATE TABLE test2 LIKE test;

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
mysql> CREATE TABLE source(a UInt64 null, b Varchar null)

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

mysql> CREATE TABLE copy2(x VARCHAR NULL, y VARCHAR NULL) AS SELECT * FROM source;

mysql> SELECT * FROM copy2;
+------+------+------+-------+
| x    | y    | a    | b     |
+------+------+------+-------+
| NULL | NULL |  888 | stars |
+------+------+------+-------+
```
