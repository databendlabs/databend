---
title: CEIL
description: CEIL() function
---

Rounds the number up.

## Syntax

```sql
CEIL(x)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| x | The numerical value. |

## Return Type

A numeric data type value which is the same as input type.

## Examples

```sql
SELECT CEIL(1.23), typeof(ceil(3)), typeof(ceil(5.3));
+------------+------------------+-------------------+
| CEIL(1.23) | typeof(ceil(3))  | typeof(ceil(5.3)) |
+------------+------------------+-------------------+
|        2.0 | TINYINT UNSIGNED | DOUBLE            |
+------------+------------------+-------------------+

SELECT CEIL(-1.23);
+----------------+
| CEIL((- 1.23)) |
+----------------+
|             -1 |
+----------------+
```
