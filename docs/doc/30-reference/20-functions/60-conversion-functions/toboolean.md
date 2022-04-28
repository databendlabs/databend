---
title: toBoolean
---

toBoolean function is used to converts a specified value to an equivalent boolean value.

## Syntax

```sql
toBoolean(expr) — Results in the bool data type.
```

## Examples

```sql
MySQL [(none)]> select toBoolean(true);
+-----------------+
| toBoolean(true) |
+-----------------+
|               1 |
+-----------------+
```
```sql
MySQL [(none)]> select toBoolean(false);
+------------------+
| toBoolean(false) |
+------------------+
|                0 |
+------------------+
```
```sql
MySQL [(none)]> select toBoolean(1);
+--------------+
| toBoolean(1) |
+--------------+
|            1 |
+--------------+
```