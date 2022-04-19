---
title: CREATE TABLE
description: Create a new table.
---

`CREATE TABLE` is the most complicated part of many Databases, you need to:
* Manually specify the engine
* Manually specify the indexes
* And even specify the data partitions or data shard
 
In Databend, you **don't need to specify any of these**, one of Databend's design goals is to make it easier to use.

## Syntax

### Create Table
```text
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
* [Numeric Data Types](../../../10-data-types/10-data-type-numeric-types.md)
* [Date & Time Data Types](../../../10-data-types/20-data-type-time-date-types.md)
* [String Data Types](../../../10-data-types/30-data-type-string-types.md)
* [Semi-structured Data Types](../../../10-data-types/40-data-type-semi-structured-types.md)
:::

### Create Table LIKE

Creates an empty copy of an existing table, the new table automatically copies all column names, their data types, and their not-null constraints.

Syntax:
```text
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
```

### Create Table AS [SELECT query]

Creates a table and fills it with data computed by a SELECT command.

```text
CREATE TABLE [IF NOT EXISTS] [db.]table_name
LIKE [db.]origin_table_name
AS SELECT query
```

## Column Nullable

By default, **all columns are not nullable(NOT NULL)**, if you want to specify a column default to `NULL`, please use:
```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    <column_name> <data_type> NULL,
     ...
)
```

Let check it out how difference the column is `NULL` or `NOT NULL`.

Create a table `t_not_null` which column with `NOT NULL`(Databend Column is `NOT NULL` by default):
```sql
CREATE TABLE t_not_null(a INT);
```

```sql
DESC t_not_null;
+-------+-------+------+---------+
| Field | Type  | Null | Default |
+-------+-------+------+---------+
| a     | Int32 | NO   | 0       |
+-------+-------+------+---------+
```

Create another table `t_null` column with `NULL`:
```sql
CREATE TABLE t_null(a INT NULL);
```

```sql
DESC t_null;
+-------+-------+------+---------+
| Field | Type  | Null | Default |
+-------+-------+------+---------+
| a     | Int32 | YES  | NULL    |
+-------+-------+------+---------+
```

## Default Values
```text
DEFAULT <expression>
```
Specifies a default value inserted in the column if a value is not specified via an INSERT or CREATE TABLE AS SELECT statement.

For example:
```sql
CREATE TABLE t_default_value(a TINYINT UNSIGNED, b SMALLINT DEFAULT (a+3), c VARCHAR DEFAULT 'c');
```

Desc the `t_default_value` table:
```sql
DESC t_default_value;
+-------+--------+------+---------+
| Field | Type   | Null | Default |
+-------+--------+------+---------+
| a     | UInt8  | NO   | 0       |
| b     | Int16  | NO   | (a + 3) |
| c     | String | NO   | c       |
+-------+--------+------+---------+
```

Insert a value:
```sql
INSERT INTO T_default_value(a) VALUES(1);
```

Check the table values:
```sql
SELECT * FROM t_default_value;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |    4 | c    |
+------+------+------+
```

## MySQL Compatibility

Databend’s syntax is difference from MySQL mainly in the data type and some specific index hints.

## Examples

### Create Table

```sql
CREATE TABLE test(a BIGINT UNSIGNED, b VARCHAR , c VARCHAR  DEFAULT concat(b, '-b'));
```

```sql
DESC test;
+-------+--------+------+---------------+
| Field | Type   | Null | Default       |
+-------+--------+------+---------------+
| a     | UInt64 | NO   | 0             |
| b     | String | NO   |               |
| c     | String | NO   | concat(b, -b) |
+-------+--------+------+---------------+
```

```sql
INSERT INTO test(a,b) VALUES(888, 'stars');
```

```sql
SELECT * FROM test;
+------+-------+---------+
| a    | b     | c       |
+------+-------+---------+
|  888 | stars | stars-b |
+------+-------+---------+
```

### Create Table Like Statement
```sql
CREATE TABLE test2 LIKE test;
```

```sql
DESC test2;
+-------+--------+------+---------------+
| Field | Type   | Null | Default       |
+-------+--------+------+---------------+
| a     | UInt64 | NO   | 0             |
| b     | String | NO   |               |
| c     | String | NO   | concat(b, -b) |
+-------+--------+------+---------------+
```

```sql
INSERT INTO test2(a,b) VALUES(888, 'stars');
```

```sql
SELECT * FROM test2;
+------+-------+---------+
| a    | b     | c       |
+------+-------+---------+
|  888 | stars | stars-b |
+------+-------+---------+
```

### Create Table As SELECT (CTAS) Statement

```sql
CREATE TABLE test3 AS SELECT * FROM test2;
```
```sql
DESC test3;
+-------+--------+------+---------------+
| Field | Type   | Null | Default       |
+-------+--------+------+---------------+
| a     | UInt64 | NO   | 0             |
| b     | String | NO   |               |
| c     | String | NO   | concat(b, -b) |
+-------+--------+------+---------------+
```

```sql
SELECT * FROM test3;
+------+-------+---------+
| a    | b     | c       |
+------+-------+---------+
|  888 | stars | stars-b |
+------+-------+---------+
```
