---
title: AVG_IF
---


## AVG_IF 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
AVG_IF(column, cond)
```

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT avg(number) FROM numbers(10);
+-------------+
| avg(number) |
+-------------+
|         4.5 |
+-------------+

SELECT avg_if(number, number > 7) FROM numbers(10);
+-----------------------------+
| avgIf(number, (number > 7)) |
+-----------------------------+
|                         8.5 |
+-----------------------------+
```
