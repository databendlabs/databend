---
title: NULLIF
description: 'NULLIF( <expr1>, <expr2> ) function'
---

The NULLIF() function compares two expressions and returns NULL if they are equal. Otherwise, the first expression is returned.

:::tip

Before using this function, you must enable the new Databend planner. To do so, perform the following command in the SQL client:

```sql
> set enable_planner_v2=1;
```
:::

## Syntax

```sql
IF( <expr1>, <expr2>)
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

SELECT a, b, NULLIF(a, b) FROM t;
+------+------+--------------+
| a    | b    | NULLIF(a, b) |
+------+------+--------------+
|    0 |    0 |         NULL |
|    0 |    1 |            0 |
|    0 | NULL |            0 |
|    1 |    0 |            1 |
|    1 |    1 |         NULL |
|    1 | NULL |            1 |
| NULL |    0 |         NULL |
| NULL |    1 |         NULL |
| NULL | NULL |         NULL |
+------+------+--------------+
```
