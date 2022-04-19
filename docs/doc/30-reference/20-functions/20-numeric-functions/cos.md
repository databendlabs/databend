---
title: COS
description: COS(x) function
---

Returns the cosine of x, where x is given in radians.

## Syntax

```sql
COS(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.


## Examples

```sql
SELECT COS(PI());
+-----------+
| COS(PI()) |
+-----------+
|        -1 |
+-----------+
```
