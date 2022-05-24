---
title: SUM_IF
---


## SUM_IF

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
SUM_IF(column, cond)
```

## Examples

:::tip numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1. :::

```sql
SELECT sum(number) FROM numbers(10);
+-------------+
| sum(number) |
+-------------+
|          45 |
+-------------+

SELECT sum_if(number, number > 7) FROM numbers(10);
+------------------------------+
| sum_if(number, (number > 7)) |
+------------------------------+
|                           17 |
+------------------------------+
```
