---
title: Nullable
description: Nullable data type.
---

## Nullable Data Types

`NULL` values are used to represent nonexistent or unknown values.
`Nullable` data types allow storing `NULL` values.
For example, the `Nullable(Int64)` type can store `Int64` values as well as `NULL`.

## Syntax

Define column field data types `Nullable` while creating a table is as follows:

```sql
CREATE TABLE test(
    id Int64,
    name String NULL,
    age Int32 NOT NULL
);
```

`NULL` indicates that the data type of this field is `Nullable` and can store `NULL` values.
`NOT NULL` indicates that this field cannot store `NULL` values.

:::note
Using `Nullable` will almost always have a negative impact on performance. If there is no explicit setting, Databend's column field data type defaults to `NOT NULL`.
:::

## Functions

Check whether the value is `NULL` or `NOT NULL`.

[IS NULL](/doc/reference/functions/conditional-functions/isnull)
[IS NOT NULL](/doc/reference/functions/conditional-functions/isnotnull)

### Example

```sql
CREATE TABLE nullable_table(a Int8 NOT NULL, b Int8 NULL);

DESC nullable_table;
+-------+---------+------+---------+-------+
| Field | Type    | Null | Default | Extra |
+-------+---------+------+---------+-------+
| a     | TINYINT | NO   | 0       |       |
| b     | TINYINT | YES  | NULL    |       |
+-------+---------+------+---------+-------+

INSERT INTO nullable_table VALUES(1, NULL),(2, 3);

SELECT * FROM nullable_table;
+---+--------+
| a | b      |
+---+--------+
| 1 | <null> |
| 2 | 3      |
+---+--------+

SELECT b, b IS NULL, b IS NOT NULL FROM nullable_table;
+--------+-----------+---------------+
| b      | b IS NULL | b IS NOT NULL |
+--------+-----------+---------------+
| <null> | 1         | 0             |
| 3      | 0         | 1             |
+--------+-----------+---------------+
```
