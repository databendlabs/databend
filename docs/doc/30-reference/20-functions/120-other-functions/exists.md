---
title: exists
---

The exists condition is used in combination with a subquery and is considered "to be met" if the subquery returns at least one row.

## Syntax

```sql
where exists ( subquery );
```

## Examples
```sql
MySQL [(none)]> select number from numbers(5) as A where exists (select * from numbers(3) where number=1); 
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
|      4 |
+--------+
```
```sql
MySQL [(none)]> select number from numbers(5) as A where exists (select * from numbers(3) where number=4); 
Query OK, 0 rows affected (0.04 sec)
```

