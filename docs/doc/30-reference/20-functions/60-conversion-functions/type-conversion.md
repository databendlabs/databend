---
title: Type Conversion
---

toInt(8|16|32|64)

## Syntax

```sql
toInt8(expr) — Results in the Int8 data type.
toInt16(expr) — Results in the Int16 data type.
toInt32(expr) — Results in the Int32 data type.
toInt64(expr) — Results in the Int64 data type.
```

## Examples

```sql
mysql> SELECT toInt8(1);
+-----------+
| toInt8(1) |
+-----------+
|         1 |
+-----------+

mysql> SELECT toTypeName(toInt8(1));
+-----------------------+
| toTypeName(toInt8(1)) |
+-----------------------+
| Int8                  |
+-----------------------+

```
