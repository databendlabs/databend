---
title: EXP
---

Returns the value of e (the base of natural logarithms) raised to the power of x.

## Syntax

```sql
EXP(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A Float64 data type value.


## Examples

```sql
mysql> SELECT EXP(2);
+------------------+
| EXP(2)           |
+------------------+
| 7.38905609893065 |
+------------------+
1 row in set (0.00 sec)

mysql> SELECT EXP(-2);
+--------------------+
| EXP((- 2))         |
+--------------------+
| 0.1353352832366127 |
+--------------------+
1 row in set (0.00 sec)

mysql> SELECT EXP(0);
+--------+
| EXP(0) |
+--------+
|      1 |
+--------+
1 row in set (0.01 sec)
```
