---
title: QUANTILE
---

Aggregate function.

The QUANTILE() function computes the quantile of a numeric data sequence.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
QUANTILE(level)(expression)
```

## Arguments

| Arguments   | Description                                                                                                                  |
| ----------- |------------------------------------------------------------------------------------------------------------------------------|
| level       | level of quantile. Constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99] |
| ----------- | -----------                                                                                                                  |
| expression  | Any numerical expression                                                                                                     |

## Return Type

the type of the value.

## Examples

:::tip
QUANTILE(0.6)(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT QUANTILE(0.6)(number) FROM numbers(10000);
+-----------------------+
| quantile(0.6)(number) |
+-----------------------+
|       5999            |
+-----------------------+
```
