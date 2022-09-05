---
title: ANY
---

Aggregate function.

The ANY() function selects the first encountered (non-NULL) value, unless all rows have NULL values in that column. The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate. To get a determinate result, you can use the ‘min’ or ‘max’ function instead of ‘any’.

## Syntax

```
ANY(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The first encountered (non-NULL) value, in the type of the value. If all values are NULL, the return value is NULL.

## Examples

:::tip
    numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT ANY(number) FROM numbers(3);
+-------------+
| any(number) |
+-------------+
|           0 |
+-------------+

-- Table t1:
-- +------+
-- | a    |
-- +------+
-- | NULL |
-- | NULL |
-- |    1 |
-- |    2 |
-- | NULL |
-- |    3 |
-- +------+
SELECT ANY(a) FROM t1;
+--------+
| any(a) |
+--------+
|      1 |
+--------+

-- Table t2:
-- +------+
-- | a    |
-- +------+
-- | NULL |
-- | NULL |
-- | NULL |
-- +------+
SELECT ANY(a) FROM t1;
+--------+
| any(a) |
+--------+
|   NULL |
+--------+
```
