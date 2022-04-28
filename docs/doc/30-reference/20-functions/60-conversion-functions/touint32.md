---
title: toUInt32
---

toUInt32 function is used to convert a specified value to a 32-bit unsigned integer.

## Syntax

```sql
toUInt32(expr) — Results in the uint32 data type.
```

## Examples

```sql
MySQL [(none)]> select toUInt32(123);
+---------------+
| toUInt32(123) |
+---------------+
|           123 |
+---------------+
```
```sql
MySQL [(none)]> select toUInt32('123');
+-----------------+
| toUInt32('123') |
+-----------------+
|             123 |
+-----------------+
```