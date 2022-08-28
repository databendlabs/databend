---
title: CEILING
description: CEILING(x) function
---

CEILING() is a synonym for CEIL().

Returns the smallest numeric value not less than x.

## Syntax

```sql
CEILING(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A numeric data type value which is the same as input type.

## Examples

```sql
SELECT CEILING(1.23);
+---------------+
| CEILING(1.23) |
+---------------+
|           2.0 |
+---------------+

SELECT CEILING(-1.23);
+-----------------+
| CEILING(- 1.23) |
+-----------------+
|            -1.0 |
+-----------------+
```
