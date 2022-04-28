---
title: EXP
description: EXP(x) function
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
SELECT EXP(2);
+------------------+
| EXP(2)           |
+------------------+
| 7.38905609893065 |
+------------------+

SELECT EXP(-2);
+--------------------+
| EXP((- 2))         |
+--------------------+
| 0.1353352832366127 |
+--------------------+

SELECT EXP(0);
+--------+
| EXP(0) |
+--------+
|      1 |
+--------+
```
