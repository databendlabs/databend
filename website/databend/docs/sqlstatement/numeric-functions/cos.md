---
id: numeric-cos
title: COS
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

```
mysql> SELECT COS(PI());
+-----------+
| COS(PI()) |
+-----------+
|        -1 |
+-----------+
1 row in set (0.00 sec)
Read 1 rows, 1 B in 0.000 sec., 2.64 thousand rows/sec., 2.64 KB/sec.
```
