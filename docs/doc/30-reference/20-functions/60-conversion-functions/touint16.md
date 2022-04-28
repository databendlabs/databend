---
title: toUInt16
---

toUInt16 function is used to convert a specified value to a 16-bit unsigned integer.

## Syntax

```sql
toUInt16(expr) — Results in the uint16 data type.
```

## Examples

```sql
MySQL [(none)]> select toUInt16(123);
+---------------+
| toUInt16(123) |
+---------------+
|           123 |
+---------------+
```
```sql
MySQL [(none)]> select toUInt16('123');
+-----------------+
| toUInt16('123') |
+-----------------+
|             123 |
+-----------------+
```