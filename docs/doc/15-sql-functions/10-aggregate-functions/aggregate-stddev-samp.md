---
title: STDDEV_SAMP
---

Aggregate function.

The STDDEV_SAMP() function returns the sample standard deviation(the square root of VAR_SAMP()) of an expression.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
STDDEV_SAMP(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any numerical expression |

## Return Type

double

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT STDDEV_SAMP(number) FROM numbers(10000);
+---------------------+
| stddev_samp(number) |
+---------------------+
|  2886.8956799071675 |
+---------------------+

```
