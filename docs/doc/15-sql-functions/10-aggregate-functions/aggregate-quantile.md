---
title: QUANTILE_DISC
---

Aggregate function.

The `QUANTILE_DISC()` function computes the exact quantile number of a numeric data sequence.
The `QUANTILE` alias to `QUANTILE_DISC`

:::caution
NULL values are not counted.
:::

## Syntax

```sql
QUANTILE_DISC(level)(expression)
    
QUANTILE_DISC(level1, level2, ...)(expression)
```

## Arguments

| Arguments   | Description                                                                                                                                   |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| level(s)    | level(s) of quantile. Each level is constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99] |
| ----------- | -----------                                                                                                                                   |
| expression  | Any numerical expression                                                                                                                      |

## Return Type

InputType or array of InputType based on level number.

## Examples

:::tip
QUANTILE_DISC(0.6)(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT QUANTILE_DISC(0.6)(number) FROM numbers(10000);
+----------------------------+
| QUANTILE_DISC(0.6)(number) |
+----------------------------+
|       5999                 |
+----------------------------+
```

```sql
SELECT QUANTILE_DISC(0, 0.5, 0.6, 1)(number) FROM numbers_mt(10000);
+---------------------------------------+
| QUANTILE_DISC(0, 0.5, 0.6, 1)(number) |
+---------------------------------------+
|      [0, 4999 ,5999 ,9999 ]           |
+---------------------------------------+
```
