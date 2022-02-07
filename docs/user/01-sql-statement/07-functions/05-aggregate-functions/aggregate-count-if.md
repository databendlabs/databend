---
title: countIf
---


## countIf 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
countIf(column, cond)
```

## Examples

:::note
numbers_mt(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT count(number) FROM numbers(10);
+---------------+
| count(number) |
+---------------+
|            10 |
+---------------+

mysql> SELECT countIf(number, number > 7) FROM numbers(10);
+-------------------------------+
| countIf(number, (number > 7)) |
+-------------------------------+
|                             2 |
+-------------------------------+
```
