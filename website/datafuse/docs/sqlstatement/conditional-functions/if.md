---
id: conditional-if
title: IF
---

If expr1 is TRUE, IF() returns expr2. Otherwise, it returns expr3.

## Syntax

```sql
IF(expr1,expr2,expr3)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr1 | The condition for evaluation that can be true or false. |
| expr2 | The expression to return if condition is met. |
| expr3 | The expression to return if condition is not met. |

## Return Type

The return type is determined by expr2 and expr3， they must have the lowest common type.

## Examples

```
mysql> select if(number=0, true, false) from numbers(1);
+-------------------------------+
| if((number = 0), true, false) |
+-------------------------------+
|                          true |
+-------------------------------+
1 row in set (0.01 sec)

```
