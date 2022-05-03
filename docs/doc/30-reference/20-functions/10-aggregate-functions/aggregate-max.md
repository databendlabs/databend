---
title: MAX
---

Aggregate function.

The MAX() function returns the maximum value in a set of values.

## Syntax

```
MAX(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The maximum value, in the type of the value.

## Examples

:::tip
    numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT MAX(*) FROM numbers(3);
+--------+
| max(*) |
+--------+
|      2 |
+--------+

SELECT MAX(number) FROM numbers(3);
+-------------+
| max(number) |
+-------------+
|           2 |
+-------------+

SELECT MAX(number) AS max FROM numbers(3);
+------+
| max  |
+------+
|    2 |
+------+
```
