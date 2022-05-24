---
title: AVG
---

Aggregate function.

The AVG() function returns the average value of an expression.

**Note:** NULL values are not counted.

## Syntax

```sql
AVG(expression)
```

## Arguments

| Arguments  | Description              |
| ---------- | ------------------------ |
| expression | Any numerical expression |

## Return Type

double

## Examples

:::tip numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1. :::

```sql
SELECT AVG(*) FROM numbers(3);
+--------+
| avg(*) |
+--------+
|      1 |
+--------+

SELECT AVG(number) FROM numbers(3);
+-------------+
| avg(number) |
+-------------+
|           1 |
+-------------+

SELECT AVG(number+1) FROM numbers(3);
+----------------------+
| avg(plus(number, 1)) |
+----------------------+
|                    2 |
+----------------------+

SELECT AVG(number+1) AS a FROM numbers(3);
+------+
| a    |
+------+
|    2 |
+------+
```
