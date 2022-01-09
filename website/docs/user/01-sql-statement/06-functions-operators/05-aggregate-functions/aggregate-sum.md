---
title: SUM
---

Aggregate function.

The SUM() function calculates the sum of a set of values.

**Note:** NULL values are not counted.

## Syntax

```
SUM(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

A double if the input type is double, otherwise integer.

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
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
