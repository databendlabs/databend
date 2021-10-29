---
id: datetime-tostartofisoyear
title: toStartOfISOYear
---

Returns the first day of the ISO year for a date or a date with time.
## Syntax

```sql
toStartOfISOYear(expr)
```

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
mysql> select toStartOfISOYear(toDate(18869));
+---------------------------------+
| toStartOfISOYear(toDate(18869)) |
+---------------------------------+
| 2021-01-04                      |
+---------------------------------+

mysql> select toStartOfISOYear(toDateTime(1630812366));
+------------------------------------------+
| toStartOfISOYear(toDateTime(1630812366)) |
+------------------------------------------+
| 2021-01-04                               |
+------------------------------------------+
```