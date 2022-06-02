---
title: SIN
description: SIN(x) function
---

Returns the sine of x, where x is given in radians.

## Syntax

```sql
SIN(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT SIN(PI());
+------------------------------------+
| SIN(PI())                          |
+------------------------------------+
| 0.00000000000000012246467991473532 |
+------------------------------------+
```
