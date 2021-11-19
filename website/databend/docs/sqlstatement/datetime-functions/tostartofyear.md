---
id: datetime-tostartofyear
title: toStartOfYear
---

Returns the first day of the year for a date or a date with time.
## Syntax

```sql
toStartOfYear(expr)
```

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
select toStartOfYear(toDate(18869));
+------------------------------+
| toStartOfYear(toDate(18869)) |
+------------------------------+
| 2021-01-01                   |
+------------------------------+

mysql> select toStartOfYear(toDateTime(1630812366));
+---------------------------------------+
| toStartOfYear(toDateTime(1630812366)) |
+---------------------------------------+
| 2021-01-01                            |
+---------------------------------------+
```