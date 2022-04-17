---
title: MAX_IF
---

## MAX_IF

The suffix `_IF` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
MAX_IF(column, cond)
```

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT max(number) FROM numbers(10);
+-------------+
| max(number) |
+-------------+
|           9 |
+-------------+

mysql> SELECT max_if(number, number < 7) FROM numbers(10);
+------------------------------+
| max_if(number, (number < 7)) |
+------------------------------+
|                            6 |
+------------------------------+
```
