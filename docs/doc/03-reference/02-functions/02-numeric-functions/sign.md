---
title: SIGN
---

Returns the sign of the argument as -1, 0, or 1, depending on whether X is negative, zero, or positive or NULL if the argument was NULL.

## Syntax

```sql
SIGN(X)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| X | The numerical value. |

## Return Type

A i8 data type value.

## Examples

```text
mysql> SELECT SIGN(NULL);
+------------+
| SIGN(NULL) |
+------------+
|       NULL |
+------------+

mysql> SELECT SIGN(0);
+---------+
| SIGN(0) |
+---------+
|       0 |
+---------+

mysql> SELECT SIGN(10);
+----------+
| SIGN(10) |
+----------+
|        1 |
+----------+

mysql> SELECT SIGN(-10);
+--------------+
| SIGN((- 10)) |
+--------------+
|           -1 |
+--------------+

```
