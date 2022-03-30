---
title: timeslot
---

Rounds the time to the half hour.
## Syntax

```sql
timeslot(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
mysql> select timeslot(now());
+---------------------+
| timeslot(now())     |
+---------------------+
| 2022-03-29 06:30:00 |
+---------------------+

mysql> select timeslot(toDateTime(1630812366));
+----------------------------------+
| timeslot(toDateTime(1630812366)) |
+----------------------------------+
| 2021-09-05 03:00:00              |
+----------------------------------+
```
