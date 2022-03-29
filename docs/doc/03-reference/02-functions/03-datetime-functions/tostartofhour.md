---
title: toStartOfHour
---

Rounds down a date with time to the start of the hour.
## Syntax

```sql
toStartOfHour(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
mysql> select toStartOfHour(now());
+----------------------+
| toStartOfHour(now()) |
+----------------------+
| 2022-03-29 06:00:00  |
+----------------------+

mysql> select toStartOfHour(toDateTime(1630812366));
+---------------------------------------+
| toStartOfHour(toDateTime(1630812366)) |
+---------------------------------------+
| 2021-09-05 03:00:00                   |
+---------------------------------------+
```
