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
DESC system.one;
+-------+------------------+------+---------+
| Field | Type             | Null | Default |
+-------+------------------+------+---------+
| dummy | TINYINT UNSIGNED | NO   | 0       |
+-------+------------------+------+---------+
```
