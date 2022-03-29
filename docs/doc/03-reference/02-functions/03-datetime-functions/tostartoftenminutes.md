---
title: toStartOfTenMinutes
---

Rounds down a date with time to the start of the ten-minute interval.
## Syntax

```sql
toStartOfTenMinutes(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
mysql> select toStartOfTenMinutes(now());
+----------------------------+
| toStartOfTenMinutes(now()) |
+----------------------------+
| 2022-03-29 06:40:00        |
+----------------------------+

mysql> select toStartOfTenMinutes(toDateTime(1630812366));
+---------------------------------------------+
| toStartOfTenMinutes(toDateTime(1630812366)) |
+---------------------------------------------+
| 2021-09-05 03:20:00                         |
+---------------------------------------------+
```
