---
title: ATAN2
---

Returns the arc tangent of the two variables x and y. It is similar to calculating the arc tangent of y / x, except that the signs of both arguments are used to determine the quadrant of the result.
ATAN(y, x) is a synonym for ATAN2(y, x).

## Syntax

```sql
ATAN2(y, x)
ATAN(y, x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| y | The angle, in radians. |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT ATAN2(-2, 2);
+---------------------+
| ATAN2((- 2), 2)     |
+---------------------+
| -0.7853981633974483 |
+---------------------+
1 row in set (0.01 sec)
```

