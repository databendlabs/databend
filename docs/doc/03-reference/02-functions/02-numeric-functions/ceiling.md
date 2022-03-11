---
title: CEILING
---

Returns the smallest integer value not less than x.

## Syntax

```sql
CEILING(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT CEILING(1.23);
+---------------+
| CEILING(1.23) |
+---------------+
|             2 |
+---------------+
1 row in set (0.01 sec)

mysql> SELECT CEILING(-1.23);
+-------------------+
| CEILING((- 1.23)) |
+-------------------+
|                -1 |
+-------------------+
1 row in set (0.00 sec)
```
