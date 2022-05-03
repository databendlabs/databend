---
title: Comparison Operators
title_includes: =, >=, >, !=, <=, <, <>
---
Comparison operators.

## Syntax

| Operator | Syntax             |  Description
| -------- | ------------------ |  ----------
| `=`        |  `a = b`         |  a is equal to b.
| `!=`       |  `a != b`        |  a is not equal to b.
| `<>`       |  `a <> b`        |  a is not equal to b.
| `>`        |  `a > b`         |  a is greater than b.
| `>=`       |  `a >= b`        |  a is greater than or equal to b.
| `<`        |  `a < b`         |  a is less than b.
| `<=`       |  `a <= b`        |  a is less than or equal to b.

## Examples

```sql
SELECT 1=1;
+---------+
| (1 = 1) |
+---------+
|       1 |
+---------+

SELECT 2>=1;
+----------+
| (2 >= 1) |
+----------+
|        1 |
+----------+

SELECT 2>1;
+---------+
| (2 > 1) |
+---------+
|       1 |
+---------+

SELECT 2 <= 1;
+----------+
| (2 <= 1) |
+----------+
|        0 |
+----------+

SELECT 1 < 2;
+---------+
| (1 < 2) |
+---------+
|       1 |
+---------+

SELECT '.01' != '0.01';
+-------------------+
| ('.01' <> '0.01') |
+-------------------+
|                 1 |
+-------------------+

SELECT '.01' <> '0.01';
+-------------------+
| ('.01' <> '0.01') |
+-------------------+
|                 1 |
+-------------------+
```