---
title: COVAR_SAMP
---

Aggregate function.

The covar_samp() function returns the sample covariance (Σ((x - x̅)(y - y̅)) / (n - 1)) of two data columns.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
COVAR_SAMP(expr0, expr1)
```

## Arguments

| Arguments   | Description              |
| ----------- | ------------------------ |
| expression0 | Any numerical expression |
| expression1 | Any numerical expression |

## Return Type

float64, when n <= 1, returns +∞.

## Examples

```sql
SELECT covar_samp(number, number) from (SELECT * from numbers_mt(2) order by number asc);
+----------------------------+
| covar_samp(number, number) |
+----------------------------+
|                        0.5 |
+----------------------------+

```
