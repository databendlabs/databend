---
title: toHour
---

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23).
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## Syntax

```sql
toHour(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
A `UInt8` number datatype.

## Examples

```sql
mysql> select toHour(now());
+---------------+
| toHour(now()) |
+---------------+
|             6 |
+---------------+

mysql> select toHour(toDateTime(1630812366));
+--------------------------------+
| toHour(toDateTime(1630812366)) |
+--------------------------------+
|                              3 |
+--------------------------------+
```
