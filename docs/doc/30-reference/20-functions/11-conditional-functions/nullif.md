---
title: NULLIF
description: 'NULLIF( <expr1>, <expr2> ) function'
---

The NULLIF() function compares two expressions and returns NULL if they are equal. Otherwise, the first expression is returned.

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
