---
title: LOG functions
description: LOG functions
title_includes: LOG, LOG2, LOG10, LN
---

LOG: If called with one parameter, this function returns the natural logarithm of x. If x is less than or equal to 0.0E0, the function returns NULL. 

LOG2: Returns the base-2 logarithm of x. If x is less than or equal to 0.0E0, the function returns NULL. 

LOG10: Returns the base-10 logarithm of x. If x is less than or equal to 0.0E0, the function returns NULL. 

LN: Returns the natural logarithm of x; that is, the base-e logarithm of x. If x is less than or equal to 0.0E0, the function returns NULL. 

## Syntax

```sql
LOG(x)
LOG(b, x)
LOG2(x)
LOG10(x)
LN(x)
```

## Examples

```sql
MySQL [(none)]> select LOG(2);
+--------------------+
| LOG(2)             |
+--------------------+
| 0.6931471805599453 |
+--------------------+

MySQL [(none)]> select LOG(-2);
+---------+
| LOG(-2) |
+---------+
|     NaN |
+---------+

MySQL [(none)]> select LOG(2, 65536);
+---------------+
| LOG(2, 65536) |
+---------------+
|            16 |
+---------------+

MySQL [(none)]> select LOG2(65536);
+-------------+
| LOG2(65536) |
+-------------+
|          16 |
+-------------+

MySQL [(none)]> select LOG10(100);
+------------+
| LOG10(100) |
+------------+
|          2 |
+------------+

MySQL [(none)]> select LN(2);
+--------------------+
| LN(2)              |
+--------------------+
| 0.6931471805599453 |
+--------------------+
```

