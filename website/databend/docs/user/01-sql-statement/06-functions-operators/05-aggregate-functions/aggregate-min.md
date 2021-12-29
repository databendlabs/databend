---
title: MIN
---

Aggregate function.

The MIN() function returns the minimum value in a set of values.

## Syntax

```
MIN(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The minimum value, in the type of the value.

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

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

