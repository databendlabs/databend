---
title: toDateTime
---

toDateTime function is used to convert a specified value to datetime.

## Syntax

```sql
toDateTime(expr) — Results in the datetime data type.
```

## Examples

```sql
MySQL [(none)]> select toDateTime(1651036648000000);
+------------------------------+
| toDateTime(1651036648000000) |
+------------------------------+
| 2022-04-27 05:17:28.000000   |
+------------------------------+
```