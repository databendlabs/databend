---
title: avgIf
---


## avgIf 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
avgIf(column, cond)
```

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT avg(number) FROM numbers(10);
+-------------+
| avg(number) |
+-------------+
|         4.5 |
+-------------+

mysql> SELECT avgIf(number, number > 7) FROM numbers(10);
+-----------------------------+
| avgIf(number, (number > 7)) |
+-----------------------------+
|                         8.5 |
+-----------------------------+
```
