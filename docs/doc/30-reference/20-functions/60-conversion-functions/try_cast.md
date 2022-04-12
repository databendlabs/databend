---
title: TRY_CAST
---

Convert a value from one data type to another data type. If error happens, return NULL.

## Syntax

```sql
TRY_CAST(x AS t)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | A value to convert. |
| t | The target data type. |

## Return Type

Nullable datatype of the target data type

## Examples

```sql
mysql> SELECT TRY_CAST(1 AS VARCHAR);
+-----------------------+
| try_cast(1 as String) |
+-----------------------+
| 1                     |
+-----------------------+
1 row in set (0.020 sec)

mysql> SELECT TRY_CAST('abc' AS UInt32);
+---------------------------+
| try_cast('abc' as UInt32) |
+---------------------------+
|                      NULL |
+---------------------------+
1 row in set (0.023 sec)

mysql> SELECT toTypeName(TRY_CAST('abc' AS UInt32));
+---------------------------------------+
| toTypeName(try_cast('abc' as UInt32)) |
+---------------------------------------+
| Nullable(UInt32)                      |
+---------------------------------------+
```
