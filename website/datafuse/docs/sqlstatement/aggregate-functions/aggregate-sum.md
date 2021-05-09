---
id: aggregate-sum
title: SUM
---

Aggregate function.

The SUM() function calculates the sum of a set of values.

**Note:** NULL values are not counted.

## Syntax

```sql
SUM(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

A double if the input type is double, otherwise integer.

## Examples

```sql
mysql> SELECT SUM(*) FROM numbers(3);
+--------+
| sum(*) |
+--------+
|      3 |
+--------+

mysql> SELECT SUM(number) FROM numbers(3);
+-------------+
| sum(number) |
+-------------+
|           3 |
+-------------+

mysql> SELECT SUM(number) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    3 |
+------+

mysql> SELECT SUM(number+2) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    9 |
+------+
```
