---
title: STDDEV_POP
---

Aggregate function.

The STDDEV_POP() function returns the population standard deviation(the square root of VAR_POP()) of an expression. 

:::note
STD() or STDDEV() can also be used, which are equivalent but not standard SQL.
:::

:::caution
NULL values are not counted.
:::

## Syntax

```sql
STDDEV_POP(expression)
STDDEV(expression)
STD(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any numerical expression |

## Return Type

double

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT STDDEV_POP(number) FROM numbers(10000);
+--------------------+
| STDDEV_POP(number) |
+--------------------+
|  2886.751331514372 |
+--------------------+

mysql> SELECT STDDEV(number) FROM numbers(1000);
+--------------------+
| STDDEV(number)     |
+--------------------+
| 288.67499025720946 |
+--------------------+

mysql> SELECT STD(number) FROM numbers(100);
+-------------------+
| STD(number)       |
+-------------------+
| 28.86607004772212 |
+-------------------+

```
