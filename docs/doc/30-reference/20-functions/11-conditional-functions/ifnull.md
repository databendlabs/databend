---
title: IFNULL
description: 'IFNULL( <expr1>, <expr2> ) function'
---

The IFNULL() function return the first expression if it is not NULL. Otherwise, the second expression is returned.

IFNULL() is available under planner_v2.

## Syntax

```sql
IFNULL( <expr1>, <expr2>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr1>` | Any general expression of any data type. |
| `<expr2>` | Any general expression that evaluates to the same data type as \<expr1\>. |

## Return Type

The data type of the returned value is the data type of \<expr1\>.

## Examples

```sql
SET enable_planner_v2 = 1;

SELECT a, b, IFNULL(a, b) FROM t;
+------+------+--------------+
| a    | b    | IFNULL(a, b) |
+------+------+--------------+
|    0 |    1 |            0 |
|    0 | NULL |            0 |
| NULL |    1 |            1 |
| NULL | NULL |         NULL |
+------+------+--------------+
```
