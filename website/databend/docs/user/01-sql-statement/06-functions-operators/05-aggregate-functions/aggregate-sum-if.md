---
title: sumIf
---


## sumIf 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
sumIf(column, cond)
```

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT sum(number) FROM numbers(10);
+-------------+
| sum(number) |
+-------------+
|          45 |
+-------------+

mysql> SELECT sumIf(number, number > 7) FROM numbers(10);
+-----------------------------+
| sumIf(number, (number > 7)) |
+-----------------------------+
|                          17 |
+-----------------------------+

```
