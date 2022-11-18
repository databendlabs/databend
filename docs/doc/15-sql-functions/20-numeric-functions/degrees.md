---
title: DEGREES
description: DEGREES(x) function
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

```sql
SELECT DEGREES(PI());
+---------------+
| DEGREES(PI()) |
+---------------+
|           180 |
+---------------+

SELECT DEGREES(PI() / 2);
+---------------------+
| DEGREES((PI() / 2)) |
+---------------------+
|                  90 |
+---------------------+
```
