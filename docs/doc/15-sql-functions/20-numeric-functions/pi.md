---
title: PI
description: PI(x) function
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

```sql
SELECT PI();
+-------------------+
| PI()              |
+-------------------+
| 3.141592653589793 |
+-------------------+

SELECT PI()+1;
+-------------------+
| (PI() + 1)        |
+-------------------+
| 4.141592653589793 |
+-------------------+
```
