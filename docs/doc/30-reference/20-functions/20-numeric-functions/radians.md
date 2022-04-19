---
title: RADIANS
description: RADIANS(x) function
---

Returns the argument X, converted from degrees to radians.

## Syntax

```sql
RADIANS(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in degrees. |

## Return Type

A Float64 data type value.


## Examples

```sql
SELECT RADIANS(90);
+--------------------+
| RADIANS(90)        |
+--------------------+
| 1.5707963267948966 |
+--------------------+

SELECT RADIANS(180);
+-------------------+
| RADIANS(180)      |
+-------------------+
| 3.141592653589793 |
+-------------------+
```
