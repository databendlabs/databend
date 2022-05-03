---
title: Subtract Time Interval
description: Subtract time interval function
title_includes: subtractYears, subtractMonths, subtractDays, subtractHours, subtractMinutes, subtractSeconds
---

Subtract time interval from a date or datetime, return the result of date or datetime type.
## Syntax

```sql
subtractYears(exp0, expr1)
subtractMonths(exp0, expr1)
subtractDays(exp0, expr1)
subtractHours(exp0, expr1)
subtractMinutes(exp0, expr1)
subtractSeconds(exp0, expr1)
```

## Return Type

Date, Timestamp depends on the input.

## Examples

```sql
SELECT to_date(18875), subtractYears(to_date(18875), 2);
+---------------+---------------------------------+
| to_date(18875) | subtractYears(to_date(18875), 2) |
+---------------+---------------------------------+
| 2021-09-05    | 2019-09-05                      |
+---------------+---------------------------------+

SELECT to_date(18875), subtractMonths(to_date(18875), 2);
+---------------+----------------------------------+
| to_date(18875) | subtractMonths(to_date(18875), 2) |
+---------------+----------------------------------+
| 2021-09-05    | 2021-07-05                       |
+---------------+----------------------------------+

SELECT to_date(18875), subtractDays(to_date(18875), 2);
+---------------+--------------------------------+
| to_date(18875) | subtractDays(to_date(18875), 2) |
+---------------+--------------------------------+
| 2021-09-05    | 2021-09-03                     |
+---------------+--------------------------------+

SELECT to_datetime(1630833797), subtractHours(to_datetime(1630833797), 2);
+------------------------+------------------------------------------+
| to_datetime(1630833797) | subtractHours(to_datetime(1630833797), 2) |
+------------------------+------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 07:23:17                      |
+------------------------+------------------------------------------+

SELECT to_datetime(1630833797), subtractMinutes(to_datetime(1630833797), 2);
+------------------------+--------------------------------------------+
| to_datetime(1630833797) | subtractMinutes(to_datetime(1630833797), 2) |
+------------------------+--------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:21:17                        |
+------------------------+--------------------------------------------+

SELECT to_datetime(1630833797), subtractSeconds(to_datetime(1630833797), 2);
+------------------------+--------------------------------------------+
| to_datetime(1630833797) | subtractSeconds(to_datetime(1630833797), 2) |
+------------------------+--------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:23:15                        |
+------------------------+--------------------------------------------+
```
