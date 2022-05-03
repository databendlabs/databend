---
title: not like
---

Pattern not matching using an SQL pattern. Returns 1 (TRUE) or 0 (FALSE). If either expr or pat is NULL, the result is NULL.

## Syntax

```sql
expr NOT LIKE pat 
```

## Examples

```sql
MySQL [(none)]> select name, category from system.functions where name like 'tou%' and name not like '%64' order by name;
+----------+------------+
| name     | category   |
+----------+------------+
| touint16 | conversion |
| touint32 | conversion |
| touint8  | conversion |
+----------+------------+
```