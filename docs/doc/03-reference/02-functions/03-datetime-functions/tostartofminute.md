---
title: toStartOfMinute
---

Rounds down a date with time to the start of the minute.

## Syntax

```sql
toStartOfMinute(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
mysql> select toStartOfMinute(now());
+------------------------+
| toStartOfMinute(now()) |
+------------------------+
| 2022-03-29 06:43:00    |
+------------------------+

mysql> select toStartOfMinute(toDateTime(1630812366));
+-----------------------------------------+
| toStartOfMinute(toDateTime(1630812366)) |
+-----------------------------------------+
| 2021-09-05 03:26:00                     |
+-----------------------------------------+
```
