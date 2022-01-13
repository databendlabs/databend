---
title: argMax
---

Calculates the `arg` value for a maximum `val` value. If there are several different values of `arg` for maximum values of `val`, returns the first of these values encountered.

## Syntax

```
argMax(arg, val)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| arg | Argument |
| val | Value |

## Return Type

`arg` value that corresponds to maximum `val` value.

 matches `arg` type.

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

Input table:

```sql
mysql> SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user ORDER BY salary ASC;
+----------+------+
| salary   | user |
+----------+------+
| 16661667 |    1 |
| 16665000 |    2 |
| 16668333 |    0 |
+----------+------+
```

```sql
mysql> SELECT argMax(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_mt(10000) GROUP BY user);

+----------------------+
| argMax(user, salary) |
+----------------------+
|                    0 |
+----------------------+

```

