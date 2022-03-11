---
title: ACOS
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
mysql> SELECT ACOS(1);
+---------+
| ACOS(1) |
+---------+
|       0 |
+---------+
1 row in set (0.01 sec)

mysql> SELECT ACOS(1.0001);
+--------------+
| ACOS(1.0001) |
+--------------+
|         NULL |
+--------------+
1 row in set (0.02 sec)
```

