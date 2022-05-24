---
title: toStartOfWeek
---

Returns the first day of the year for a date or a date with time.

## Syntax

```sql
toStartOfWeek(expr)
```

## Arguments

| Arguments | Description   |
| --------- | ------------- |
| expr      | date/datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT toStartOfWeek(now());
+----------------------+
| toStartOfWeek(now()) |
+----------------------+
| 2022-03-27           |
+----------------------+

SELECT toStartOfWeek(to_datetime(1630812366));
+---------------------------------------+
| toStartOfWeek(to_datetime(1630812366)) |
+---------------------------------------+
| 2021-09-05                            |
+---------------------------------------+
```
