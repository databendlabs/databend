---
title: Add time interval
description: Add time interval function
title_includes: addYears, addMonths, addDays, addHours, addMinutes, addSeconds
---

Add time interval to a date or datetime, return the result of date or datetime type.
## Syntax

```sql
addYears(exp0, expr1)
addMonths(exp0, expr1)
addDays(exp0, expr1)
addHours(exp0, expr1)
addMinutes(exp0, expr1)
addSeconds(exp0, expr1)
```

## Return Type

Date, Timestamp, depends on the input.

## Examples

```sql
SELECT toDate(18875), addYears(toDate(18875), 2);
+---------------+-----------------------------+
| toDate(18875) | addYears(toDate(18875), 10) |
+---------------+-----------------------------+
| 2021-09-05    | 2023-09-05                  |
+---------------+-----------------------------+

SELECT toDate(18875), addMonths(toDate(18875), 2);
+---------------+-----------------------------+
| toDate(18875) | addMonths(toDate(18875), 2) |
+---------------+-----------------------------+
| 2021-09-05    | 2021-11-05                  |
+---------------+-----------------------------+

SELECT toDate(18875), addDays(toDate(18875), 2);
+---------------+---------------------------+
| toDate(18875) | addDays(toDate(18875), 2) |
+---------------+---------------------------+
| 2021-09-05    | 2021-09-07                |
+---------------+---------------------------+

SELECT toDateTime(1630833797), addHours(toDateTime(1630833797), 2);
+------------------------+-------------------------------------+
| toDateTime(1630833797) | addHours(toDateTime(1630833797), 2) |
+------------------------+-------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 11:23:17                 |
+------------------------+-------------------------------------+

SELECT toDateTime(1630833797), addMinutes(toDateTime(1630833797), 2);
+------------------------+---------------------------------------+
| toDateTime(1630833797) | addMinutes(toDateTime(1630833797), 2) |
+------------------------+---------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:25:17                   |
+------------------------+---------------------------------------+

SELECT toDateTime(1630833797), addSeconds(toDateTime(1630833797), 2);
+------------------------+---------------------------------------+
| toDateTime(1630833797) | addSeconds(toDateTime(1630833797), 2) |
+------------------------+---------------------------------------+
| 2021-09-05 09:23:17    | 2021-09-05 09:23:19                   |
+------------------------+---------------------------------------+
```
