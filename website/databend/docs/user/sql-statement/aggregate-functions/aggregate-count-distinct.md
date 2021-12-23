---
title: countDistinct
---

Aggregate function.

The count(distinct ...) function calculates the uniq value of a set of values.

**Note:** NULL values are not counted.

## Syntax

```
COUNT(distinct arguments ...)
UNIQ(arguments)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression, size of the arguments is [1, 32] |

## Return Type

UInt64

## Examples

```sql
mysql> SELECT count(distinct number % 3) FROM numbers(1000);
+------------------------------+
| count(distinct (number % 3)) |
+------------------------------+
|                            3 |
+------------------------------+

mysql>  SELECT uniq(number % 3, number) FROM numbers(1000);
+----------------------------+
| uniq((number % 3), number) |
+----------------------------+
|                       1000 |
+----------------------------+
1 row in set (0.02 sec)


mysql>  SELECT uniq(number % 3, number) = count(distinct number %3, number)  FROM numbers(1000);
+---------------------------------------------------------------------+
| (uniq((number % 3), number) = count(distinct (number % 3), number)) |
+---------------------------------------------------------------------+
|                                                                true |
+---------------------------------------------------------------------+
1 row in set (0.03 sec
```

