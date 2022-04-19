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
SELECT try_cast(1 AS VARCHAR);
+-----------------------+
| try_cast(1 as String) |
+-----------------------+
| 1                     |
+-----------------------+

SELECT try_cast('abc' AS INT UNSIGNED);
+---------------------------+
| try_cast('abc' as UInt32) |
+---------------------------+
|                      NULL |
+---------------------------+

SELECT typeof(try_cast('abc' AS INT UNSIGNED));
+-----------------------------------+
| typeof(try_cast('abc' as UInt32)) |
+-----------------------------------+
| INT UNSIGNED                      |
+-----------------------------------+
```
