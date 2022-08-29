---
title: FLOOR
description: FLOOR(x) function
---

Rounds the number down.

## Syntax

```sql
FLOOR(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A numeric data type value which is the same as input type.


## Examples

```sql
SELECT FLOOR(1.23);
+-------------+
| FLOOR(1.23) |
+-------------+
|           1 |
+-------------+

SELECT FLOOR(-1.23);
+-----------------+
| FLOOR((- 1.23)) |
+-----------------+
|              -2 |
+-----------------+
```
