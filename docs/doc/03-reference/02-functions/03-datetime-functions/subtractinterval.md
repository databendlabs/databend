---
title: subtractYEARS/MONTHS/DAYS/HOURS/MINUTES/SECONDS
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

Date16, Date32 or DateTime32, depends on the input.

## Examples

```sql
mysql> select toDate(18875), subtractYears(toDate(18875), 2);
+---------------+---------------------------------+
| toDate(18875) | subtractYears(toDate(18875), 2) |
+---------------+---------------------------------+
| 2021-09-05    | 2019-09-05                      |
+---------------+---------------------------------+

mysql> select toDate(18875), subtractMonths(toDate(18875), 2);
+---------------+----------------------------------+
| toDate(18875) | subtractMonths(toDate(18875), 2) |
+---------------+----------------------------------+
| 2021-09-05    | 2021-07-05                       |
+---------------+----------------------------------+

mysql> select toDate(18875), subtractDays(toDate(18875), 2);
+---------------+--------------------------------+
| toDate(18875) | subtractDays(toDate(18875), 2) |
+---------------+--------------------------------+
| 2021-09-05    | 2021-09-03                     |
+---------------+--------------------------------+

mysql> select toDateTime(1630833797), subtractHours(toDateTime(1630833797), 2);
+------------------------+------------------------------------------+
| toDateTime(1630833797) | subtractHours(toDateTime(1630833797), 2) |
+------------------------+------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 07:23:17                      |
+------------------------+------------------------------------------+

mysql> select toDateTime(1630833797), subtractMinutes(toDateTime(1630833797), 2);
+------------------------+--------------------------------------------+
| toDateTime(1630833797) | subtractMinutes(toDateTime(1630833797), 2) |
+------------------------+--------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:21:17                        |
+------------------------+--------------------------------------------+

mysql> select toDateTime(1630833797), subtractSeconds(toDateTime(1630833797), 2);
+------------------------+--------------------------------------------+
| toDateTime(1630833797) | subtractSeconds(toDateTime(1630833797), 2) |
+------------------------+--------------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:23:15                        |
+------------------------+--------------------------------------------+
```
