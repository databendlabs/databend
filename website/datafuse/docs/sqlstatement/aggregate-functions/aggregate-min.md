---
id: aggregate-min
title: MIN
---

Aggregate function.

The MIN() function returns the minimum value in a set of values.

## Syntax

```sql
MIN(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The minimum value, in the type of the value.

## Examples

```sql
mysql> SELECT MIN(*) FROM numbers(3);
+--------+
| min(*) |
+--------+
|      0 |
+--------+

mysql> SELECT MIN(number) FROM numbers(3);
+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+

mysql> SELECT MIN(number) AS min FROM numbers(3);
+------+
| min  |
+------+
|    0 |
+------+
```

