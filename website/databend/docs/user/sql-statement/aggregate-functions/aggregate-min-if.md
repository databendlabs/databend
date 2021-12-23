---
title: minIf
---


## minIf 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
minIf(column, cond)
```

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SELECT min(number) FROM numbers(10);
+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+

mysql> SELECT minIf(number, number > 7) FROM numbers(10);
+-----------------------------+
| minIf(number, (number > 7)) |
+-----------------------------+
|                           8 |
+-----------------------------+
```
