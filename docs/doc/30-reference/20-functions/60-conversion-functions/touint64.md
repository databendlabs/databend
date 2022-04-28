---
title: toUInt64
---

toUInt64 function is used to convert a specified value to a 64-bit unsigned integer.

## Syntax

```sql
toUInt64(expr) — Results in the uint64 data type.
```

## Examples

```sql
MySQL [(none)]> select toUInt64(123);
+---------------+
| toUInt64(123) |
+---------------+
|           123 |
+---------------+
```
```sql
MySQL [(none)]> select toUInt64('123');
+-----------------+
| toUInt64('123') |
+-----------------+
|             123 |
+-----------------+
```