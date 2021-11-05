---
id: numeric-degrees
title: DEGREES
---

Returns the argument x, converted from radians to degrees.

## Syntax

```sql
DEGREES(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.


## Examples

```
mysql> SELECT DEGREES(PI());
+---------------+
| DEGREES(PI()) |
+---------------+
|           180 |
+---------------+
1 row in set (0.01 sec)

mysql> SELECT DEGREES(PI() / 2);
+---------------------+
| DEGREES((PI() / 2)) |
+---------------------+
|                  90 |
+---------------------+
1 row in set (0.00 sec)
```
