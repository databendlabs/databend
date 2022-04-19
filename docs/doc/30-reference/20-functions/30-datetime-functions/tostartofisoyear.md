---
title: toStartOfISOYear
---

Returns the first day of the ISO year for a date or a date with time.
## Syntax

```sql
toStartOfISOYear(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT toStartOfISOYear(toDate(18869));
+---------------------------------+
| toStartOfISOYear(toDate(18869)) |
+---------------------------------+
| 2021-01-04                      |
+---------------------------------+

SELECT toStartOfISOYear(toDateTime(1630812366));
+------------------------------------------+
| toStartOfISOYear(toDateTime(1630812366)) |
+------------------------------------------+
| 2021-01-04                               |
+------------------------------------------+
```
