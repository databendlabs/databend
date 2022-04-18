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
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfTenMinutes(now());
+----------------------------+
| toStartOfTenMinutes(now()) |
+----------------------------+
| 2022-03-29 06:40:00        |
+----------------------------+

SELECT toStartOfTenMinutes(toDateTime(1630812366));
+---------------------------------------------+
| toStartOfTenMinutes(toDateTime(1630812366)) |
+---------------------------------------------+
| 2021-09-05 03:20:00                         |
+---------------------------------------------+
```
