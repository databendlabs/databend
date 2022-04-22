---
title: COUNT
---

The COUNT() function returns the number of records returned by a SELECT query.

:::caution
NULL values are not counted.
:::

## Syntax

```
COUNT(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression. <br /> This may be a column name, the result of another function, or a math operation.<br />`*` is also allowed, to indicate pure row counting.

## Return Type

An integer.

## Examples

:::tip
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
SELECT count(*) FROM numbers(3);
+----------+
| count(*) |
+----------+
|        3 |
+----------+

SELECT count(number) FROM numbers(3);
+---------------+
| count(number) |
+---------------+
|             3 |
+---------------+

SELECT count(number) AS c FROM numbers(3);
+------+
| c    |
+------+
|    3 |
+------+
```
