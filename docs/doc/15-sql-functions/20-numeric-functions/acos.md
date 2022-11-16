---
title: ACOS
description: ACOS(x) function
---

Returns the arc cosine of x, that is, the value whose cosine is x. Returns NULL if x is not in the range -1 to 1.

## Syntax

```sql
ACOS(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT ACOS(1);
+---------+
| ACOS(1) |
+---------+
|       0 |
+---------+

SELECT ACOS(1.0001);
+--------------+
| ACOS(1.0001) |
+--------------+
|         NULL |
+--------------+
```

