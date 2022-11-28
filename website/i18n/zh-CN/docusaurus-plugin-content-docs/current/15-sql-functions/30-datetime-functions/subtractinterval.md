---
title: SUBTRACT TIME INTERVAL
description: Subtract time interval function
title_includes: subtract_years, subtract_quarters, subtract_months, subtract_days, subtract_hours, subtract_minutes, subtract_seconds
---

Subtract time interval from a date or timestamp, return the result of date or timestamp type.
## Syntax

```sql
subtract_years(exp0, expr1)
subtract_quarters(exp0, expr1)
subtract_months(exp0, expr1)
subtract_days(exp0, expr1)
subtract_hours(exp0, expr1)
subtract_minutes(exp0, expr1)
subtract_seconds(exp0, expr1)
```

## Return Type

Date, Timestamp depends on the input.

## Examples

```sql
SELECT to_date(18875), subtract_years(to_date(18875), 2);
+----------------+-----------------------------------+
| to_date(18875) | subtract_years(to_date(18875), 2) |
+----------------+-----------------------------------+
| 2021-09-05     | 2019-09-05                        |
+----------------+-----------------------------------+

SELECT to_date(18875), subtract_quarters(to_date(18875), 2);
+----------------+--------------------------------------+
| to_date(18875) | subtract_quarters(to_date(18875), 2) |
+----------------+--------------------------------------+
| 2021-09-05     | 2021-03-05                           |
+----------------+--------------------------------------+

SELECT to_date(18875), subtract_months(to_date(18875), 2);
+----------------+------------------------------------+
| to_date(18875) | subtract_months(to_date(18875), 2) |
+----------------+------------------------------------+
| 2021-09-05     | 2021-07-05                         |
+----------------+------------------------------------+

SELECT to_date(18875), subtract_days(to_date(18875), 2);
+----------------+----------------------------------+
| to_date(18875) | subtract_days(to_date(18875), 2) |
+----------------+----------------------------------+
| 2021-09-05     | 2021-09-03                       |
+----------------+----------------------------------+

SELECT to_datetime(1630833797), subtract_hours(to_datetime(1630833797), 2);
+----------------------------+--------------------------------------------+
| to_datetime(1630833797)    | subtract_hours(to_datetime(1630833797), 2) |
+----------------------------+--------------------------------------------+
| 2021-09-05 09:23:17.000000 | 2021-09-05 07:23:17.000000                 |
+----------------------------+--------------------------------------------+

SELECT to_datetime(1630833797), subtract_minutes(to_datetime(1630833797), 2);
+----------------------------+----------------------------------------------+
| to_datetime(1630833797)    | subtract_minutes(to_datetime(1630833797), 2) |
+----------------------------+----------------------------------------------+
| 2021-09-05 09:23:17.000000 | 2021-09-05 09:21:17.000000                   |
+----------------------------+----------------------------------------------+

SELECT to_datetime(1630833797), subtract_seconds(to_datetime(1630833797), 2);
+----------------------------+----------------------------------------------+
| to_datetime(1630833797)    | subtract_seconds(to_datetime(1630833797), 2) |
+----------------------------+----------------------------------------------+
| 2021-09-05 09:23:17.000000 | 2021-09-05 09:23:15.000000                   |
+----------------------------+----------------------------------------------+
```
