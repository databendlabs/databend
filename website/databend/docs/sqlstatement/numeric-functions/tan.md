---
id: numeric-tan
title: TAN
---

Returns the tangent of x, where x is given in radians.

## Syntax

```sql
TAN(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.


## Examples

```
mysql> SELECT TAN(PI());
+-------------------------------------+
| TAN(PI())                           |
+-------------------------------------+
| -0.00000000000000012246467991473532 |
+-------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT TAN(PI()+1);
+--------------------+
| TAN((PI() + 1))    |
+--------------------+
| 1.5574077246549016 |
+--------------------+
1 row in set (0.00 sec)
```
