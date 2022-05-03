---
title: like
---

Pattern matching using an SQL pattern. Returns 1 (TRUE) or 0 (FALSE). If either expr or pat is NULL, the result is NULL.

## Syntax

```sql
expr LIKE pat 
```

## Examples

```sql
MySQL [(none)]>  select name, category from system.functions where name like 'tou%' order by name;
+----------+------------+
| name     | category   |
+----------+------------+
| touint16 | conversion |
| touint32 | conversion |
| touint64 | conversion |
| touint8  | conversion |
+----------+------------+
```