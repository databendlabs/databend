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
    
QUANTILE(level1, level2, ...)(expression)
```

## Arguments

| Arguments   | Description                                                                                                                                   |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| level(s)    | level(s) of quantile. Each level is constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99] |
| ----------- | -----------                                                                                                                                   |
| expression  | Any numerical expression                                                                                                                      |

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

```sql
SELECT quantile(0, 0.5, 0.6, 1)(number) from numbers_mt(10000);
+----------------------------------+
| quantile(0, 0.5, 0.6, 1)(number) |
+----------------------------------+
| [0,4999,5999,9999]               |
+----------------------------------+
```
