---
title: toSecond
---

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).

## Syntax

```sql
toSecond(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
mysql> select toSecond(now());
+-----------------+
| toSecond(now()) |
+-----------------+
|              14 |
+-----------------+

mysql> select toSecond(toDateTime(1630812366));
+----------------------------------+
| toSecond(toDateTime(1630812366)) |
+----------------------------------+
|                                6 |
+----------------------------------+
```
