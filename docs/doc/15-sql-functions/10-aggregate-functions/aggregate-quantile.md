---
title: QUANTILE
---

Aggregate function.

The QUANTILE_CONT() function computes the interpolated quantile number of a numeric data sequence.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
QUANTILE_CONT(level)(expression)
    
QUANTILE_CONT(level1, level2, ...)(expression)
```

## Arguments

| Arguments   | Description                                                                                                                                   |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| level(s)    | level(s) of quantile. Each level is constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99] |
| ----------- | -----------                                                                                                                                   |
| expression  | Any numerical expression                                                                                                                      |

## Return Type

Float64 or float64 array based on level number.

## Examples

:::tip
QUANTILE_CONT(0.6)(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT QUANTILE_CONT(0.6)(number) FROM numbers(10000);
+----------------------------+
| quantile_cont(0.6)(number) |
+----------------------------+
|       5999.4               |
+----------------------------+
```

```sql
SELECT QUANTILE_CONT(0, 0.5, 0.6, 1)(number) FROM numbers_mt(10000);
+---------------------------------------+
| quantile_cont(0, 0.5, 0.6, 1)(number) |
+---------------------------------------+
|      [0.0,4999.5,5999.4,9999.0]       |
+---------------------------------------+
```
