---
title: TO_START_OF_WEEK
---

Returns the first day of the week for a date or a date with time (timestamp/datetime).
The first day of a week can be Sunday or Monday, which is specified by the argument `mode`.

## Syntax

```sql
to_start_of_week(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date/timestamp |
| mode | Optional. If it is 0, the result is Sunday, otherwise, the result is Monday. The default value is 0 |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT to_start_of_week(now());
+-------------------------+
| to_start_of_week(now()) |
+-------------------------+
| 2022-03-27              |
+-------------------------+

SELECT to_start_of_week(to_timestamp(1630812366));
+--------------------------------------------+
| to_start_of_week(to_timestamp(1630812366)) |
+--------------------------------------------+
| 2021-09-05                                 |
+--------------------------------------------+
```
