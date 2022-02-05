---
title: POW
---

Returns the value of x raised to the power of y.

POWER() is a synonym for POW().

## Syntax

```sql
POW(x, y)
POWER(x, y)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |
| y | The numerical value. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT POW(-2,2);
+---------------+
| POW((- 2), 2) |
+---------------+
|             4 |
+---------------+
1 row in set (0.03 sec)

mysql> SELECT POW(2,-2);
+---------------+
| POW(2, (- 2)) |
+---------------+
|          0.25 |
+---------------+
1 row in set (0.03 sec)
```
