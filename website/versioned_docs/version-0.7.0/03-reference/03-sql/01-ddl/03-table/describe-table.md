---
title: DESCRIBE TABLE
---

Displays information about the columns in a given table.

## Syntax

```
DESC|DESCRIBE [database.]table_name
```

## Examples

```sql
mysql> describe system.numbers;
+--------+--------+------+
| Field  | Type   | Null |
+--------+--------+------+
| number | UInt64 | NO   |
+--------+--------+------+
1 row in set (0.01 sec)
```
