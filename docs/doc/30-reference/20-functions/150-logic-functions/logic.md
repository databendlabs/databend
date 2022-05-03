---
title: logic
title_includes: AND, OR, NOT, XOR
---

Logic functions include AND, OR, NOT, XOR. 

## Examples

```sql
MySQL [(none)]> SELECT 1 AND 1;
+-----------+
| (1 and 1) |
+-----------+
|         1 |
+-----------+

MySQL [(none)]> SELECT 1 AND 0;
+-----------+
| (1 and 0) |
+-----------+
|         0 |
+-----------+

MySQL [(none)]> SELECT 1 AND NULL;
+--------------+
| (1 and NULL) |
+--------------+
|         NULL |
+--------------+

MySQL [(none)]> SELECT 0 AND NULL;
+--------------+
| (0 and NULL) |
+--------------+
|            0 |
+--------------+

MySQL [(none)]> SELECT 1 OR 0;
+----------+
| (1 or 0) |
+----------+
|        1 |
+----------+

MySQL [(none)]> SELECT 0 OR 0;
+----------+
| (0 or 0) |
+----------+
|        0 |
+----------+

MySQL [(none)]> SELECT NOT 10;
+----------+
| (not 10) |
+----------+
|        0 |
+----------+

MySQL [(none)]> SELECT NOT 0;
+---------+
| (not 0) |
+---------+
|       1 |
+---------+

MySQL [(none)]> SELECT NOT NULL;
+------------+
| (not NULL) |
+------------+
|       NULL |
+------------+

MySQL [(none)]> SELECT 1 XOR 0;
+-----------+
| (1 xor 0) |
+-----------+
|         1 |
+-----------+

MySQL [(none)]> SELECT 1 XOR 1;
+-----------+
| (1 xor 1) |
+-----------+
|         0 |
+-----------+

MySQL [(none)]> SELECT 1 XOR NULL;
+--------------+
| (1 xor NULL) |
+--------------+
|         NULL |
+--------------+
```