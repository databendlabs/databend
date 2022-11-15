---
title: ASIN
description: ASIN(x) function
---

Returns the arc sine of x, that is, the value whose sine is x. Returns NULL if x is not in the range -1 to 1.

## Syntax

```sql
ASIN(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT ASIN(0.2);
+--------------------+
| ASIN(0.2)          |
+--------------------+
| 0.2013579207903308 |
+--------------------+

SELECT ASIN(1.1);
+-----------+
| ASIN(1.1) |
+-----------+
|      NULL |
+-----------+
```
