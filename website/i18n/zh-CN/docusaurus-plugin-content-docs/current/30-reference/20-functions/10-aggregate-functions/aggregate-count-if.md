---
title: COUNT_IF
---


## COUNT_IF

The suffix `_IF` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
COUNT_IF(column, cond)
```

## Examples

:::tip numbers_mt(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1. :::

```sql
SELECT count(number) FROM numbers(10);
+---------------+
| count(number) |
+---------------+
|            10 |
+---------------+

SELECT count_if(number, number > 7) FROM numbers(10);
+--------------------------------+
| count_if(number, (number > 7)) |
+--------------------------------+
|                              2 |
+--------------------------------+
```
