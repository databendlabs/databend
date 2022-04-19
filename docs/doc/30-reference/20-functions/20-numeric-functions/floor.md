---
title: FLOOR
description: FLOOR(x) function
---

Returns the largest integer value not greater than x.

## Syntax

```sql
FLOOR(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A Float64 data type value.


## Examples

```sql
SELECT FLOOR(1.23);
+-------------+
| FLOOR(1.23) |
+-------------+
|           1 |
+-------------+

SELECT FLOOR(-1.23);
+-----------------+
| FLOOR((- 1.23)) |
+-----------------+
|              -2 |
+-----------------+
```
