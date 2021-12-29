---
title: CEIL
---

CEIL() is a synonym for CEILING().

Returns the smallest integer value not less than x.

## Syntax

```sql
CEIL(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT CEIL(1.23);
+------------+
| CEIL(1.23) |
+------------+
|          2 |
+------------+
1 row in set (0.00 sec)

mysql> SELECT CEIL(-1.23);
+----------------+
| CEIL((- 1.23)) |
+----------------+
|             -1 |
+----------------+
1 row in set (0.00 sec)
```
