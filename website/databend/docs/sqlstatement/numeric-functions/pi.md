---
id: numeric-pi
title: PI
---

Returns the value of pi as a floating-point value.

## Syntax

```sql
PI()
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |

## Return Type

A Float64 data type value.


## Examples

```
mysql> SELECT PI();
+-------------------+
| PI()              |
+-------------------+
| 3.141592653589793 |
+-------------------+

mysql> SELECT PI()+1;
+-------------------+
| (PI() + 1)        |
+-------------------+
| 4.141592653589793 |
+-------------------+
```
