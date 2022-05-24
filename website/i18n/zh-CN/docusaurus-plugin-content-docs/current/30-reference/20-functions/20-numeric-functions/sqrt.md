---
title: SQRT
description: SQRT(x) function
---

Returns the square root of a nonnegative number x.

## Syntax

```sql
SQRT(x)
```

## Arguments

| Arguments | Description                      |
| --------- | -------------------------------- |
| x         | The nonnegative numerical value. |

## Return Type

A Float64 data type value.

## Examples

```sql
SELECT SQRT(4);
+---------+
| SQRT(4) |
+---------+
|       2 |
+---------+

SELECT SQRT(-16);
+-----------+
| SQRT(-16) |
+-----------+
|       NaN |
+-----------+
```
