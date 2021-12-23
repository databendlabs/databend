---
title: ATAN
---

Returns the arc tangent of x, that is, the value whose tangent is x.

## Syntax

```sql
ATAN(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The angle, in radians. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT ATAN(-2);
+---------------------+
| ATAN((- 2))         |
+---------------------+
| -1.1071487177940906 |
+---------------------+
1 row in set (0.02 sec)
```
