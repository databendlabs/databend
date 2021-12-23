---
title: CAST
---

Convert a value from one data type to another data type.

## Syntax

```sql
CAST(x AS t)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value to convert. |
| t | The target data type. |

## Return Type

Converted value.

## Examples

```sql
mysql> SELECT CAST(1 AS VARCHAR);
+-------------------+
| cast(1 as String) |
+-------------------+
| 1                 |
+-------------------+

mysql> SELECT CAST(1 AS UInt64);
+-------------------+
| cast(1 as UInt64) |
+-------------------+
|                 1 |
+-------------------+

mysql> SELECT toTypeName(CAST(1 AS UInt64));
+-------------------------------+
| toTypeName(cast(1 as UInt64)) |
+-------------------------------+
| UInt64                        |
+-------------------------------+

```
