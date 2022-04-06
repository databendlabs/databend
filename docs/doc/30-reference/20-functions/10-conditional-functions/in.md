---
title: IN
description: Tests whether the argument is or is not one of the members of an explicit list
---

Tests whether the argument is or is not one of the members of an explicit list.

## Syntax

```sql
<value> [ NOT ] IN ( <value1> , <value2> , ... )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |

## Return Type


## Examples

```sql
mysql> select * from numbers(10) where number in (0, 1, 2, 3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
+--------+

mysql> select * from numbers(10) where number not in (0, 1, 2, 3);
+--------+
| number |
+--------+
|      4 |
|      5 |
|      6 |
|      7 |
|      8 |
|      9 |
+--------+
```
