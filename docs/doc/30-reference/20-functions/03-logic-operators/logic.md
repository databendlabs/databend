---
title: 'Logical/Boolean Operators'
---

Logical/Boolean Operators.

## Syntax

| Operator    | Syntax             |  Description
| ----------- | ------------------ |  ----------
| `AND`       |  `a AND b`         |  Matches both expressions (`a` and `b`).
| `NOT`       |  `NOT a`           |  Does not match the expression.
| `OR`        |  `a OR b`          |  Matches either expression.


## Examples

```sql
SELECT 1 AND 1;
+-----------+
| (1 and 1) |
+-----------+
|         1 |
+-----------+

SELECT 1 AND 0;
+-----------+
| (1 and 0) |
+-----------+
|         0 |
+-----------+

SELECT 1 AND NULL;
+--------------+
| (1 and NULL) |
+--------------+
|         NULL |
+--------------+

SELECT 0 AND NULL;
+--------------+
| (0 and NULL) |
+--------------+
|            0 |
+--------------+

SELECT 1 OR 0;
+----------+
| (1 or 0) |
+----------+
|        1 |
+----------+

SELECT 0 OR 0;
+----------+
| (0 or 0) |
+----------+
|        0 |
+----------+

SELECT NOT 10;
+----------+
| (not 10) |
+----------+
|        0 |
+----------+

SELECT NOT 0;
+---------+
| (not 0) |
+---------+
|       1 |
+---------+

SELECT NOT NULL;
+------------+
| (not NULL) |
+------------+
|       NULL |
+------------+

SELECT 1 OR 0;
+-----------+
| (1 or 0)  |
+-----------+
|         1 |
+-----------+

SELECT 1 OR 1;
+-----------+
| (1 or 1)  |
+-----------+
|         0 |
+-----------+

SELECT 1 OR NULL;
+--------------+
| (1 or NULL)  |
+--------------+
|         NULL |
+--------------+
```
