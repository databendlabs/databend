---
title: toMonday
---

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## Syntax

```sql
toMonday(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
mysql> select toMonday(now());
+-----------------+
| tomonday(now()) |
+-----------------+
|           19079 |
+-----------------+

mysql> select todate(toMonday(now()));
+-------------------------+
| todate(toMonday(now())) |
+-------------------------+
| 2022-03-28              |
+-------------------------+

mysql> select toMonday(toDateTime(1630812366));
+----------------------------------+
| toMonday(toDateTime(1630812366)) |
+----------------------------------+
|                            18869 |
+----------------------------------+

```
