---
title: MEDIAN
---

Aggregate function.

The MEDIAN() function computes the median of a numeric data sequence.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
MEDIAN(expression)
```

## Arguments

| Arguments   | Description|
| ----------- | ----------- |                                                                                                                 
| expression  | Any numerical expression|                                                                                                     

## Return Type

the type of the value.

## Examples

:::tip
MEDIAN(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT MEDIAN(number) FROM numbers(10000);
+----------------+
| median(number) |
+----------------+
|    4999        |
+----------------+
```
