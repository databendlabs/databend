---
title: FLOOR
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
mysql> SELECT FLOOR(1.23);
+-------------+
| FLOOR(1.23) |
+-------------+
|           1 |
+-------------+
1 row in set (0.01 sec)

mysql> SELECT FLOOR(-1.23);
+-----------------+
| FLOOR((- 1.23)) |
+-----------------+
|              -2 |
+-----------------+
1 row in set (0.01 sec)
```
