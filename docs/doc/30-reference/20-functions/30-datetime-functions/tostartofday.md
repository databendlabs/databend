---
title: toStartOfDay
---

Rounds down a date with time to the start of the day.
## Syntax

```sql
toStartOfDay(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfDay(now());
+---------------------+
| toStartOfDay(now()) |
+---------------------+
| 2022-03-29 00:00:00 |
+---------------------+

SELECT toStartOfDay(toDateTime(1630812366));
+--------------------------------------+
| toStartOfDay(toDateTime(1630812366)) |
+--------------------------------------+
| 2021-09-05 00:00:00                  |
+--------------------------------------+
```
