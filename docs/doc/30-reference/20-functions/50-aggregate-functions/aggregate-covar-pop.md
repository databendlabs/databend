---
title: COVAR_POP
---

COVAR_POP returns the population covariance of a set of number pairs. 

## Syntax

```
COVAR_POP(expr0, expr1)
```

## Arguments

| Arguments    |        Description       |
| ------------ | ------------------------ |
| expression0  | Any numerical expression |
| expression1  | Any numerical expression |

## Return Type

float64

## Examples

```sql
SELECT covar_pop(number, number) from (select * from numbers_mt(2) order by number asc);
+---------------------------+
| covar_pop(number, number) |
+---------------------------+
|                      0.25 |
+---------------------------+
```
