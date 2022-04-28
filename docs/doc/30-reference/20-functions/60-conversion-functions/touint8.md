---
title: toUInt8
---

toUInt8 function is used to convert a specified value to a 8-bit unsigned integer.

## Syntax

```sql
toUInt8(expr) — Results in the uint8 data type.
```

## Examples

```sql
MySQL [(none)]> select toUInt8(123);
+--------------+
| toUInt8(123) |
+--------------+
|          123 |
+--------------+
```
```sql
MySQL [(none)]> select toUInt8('123');
+----------------+
| toUInt8('123') |
+----------------+
|            123 |
```