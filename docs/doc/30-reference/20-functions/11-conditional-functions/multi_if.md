---
title: MULTI_IF
description: 'MULTI_IF( <expr1>, <expr2>, <expr3> ) function'
---

MULTI_IF expr1 is TRUE, MULTI_IF( returns expr2. Otherwise, it returns expr3.

## Syntax

```sql
MULTI_IF( <cond1>, <expr1>, [<cond2>, <expr2> ..], <expr_else>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<cond_n>` | The condition for evaluation that can be true or false. |
| `<expr_n>` | The expression to return if cond_n is met. |
| `<expr_else>` | The expression to return if all conditions are not met. |

## Return Type

The return type is determined by expr2 and expr3, they must have the lowest common type.

## Examples

```sql
SELECT MULTI_IF(number=0, true, false) FROM numbers(1);
+-------------------------------+
| MULTI_IF((number = 0), true, false) |
+-------------------------------+
|                             1 |
+-------------------------------+
```

```sql
SELECT MULTI_IF(number % 3 = 1, 1, number % 3 = 2, 2, 3) FROM numbers(6);
+---------------------------------------------------+
| MULTI_IF(number % 3 = 1, 1, number % 3 = 2, 2, 3) |
+---------------------------------------------------+
|                                                 3 |
|                                                 1 |
|                                                 2 |
|                                                 3 |
|                                                 1 |
|                                                 2 |
+---------------------------------------------------+
```
