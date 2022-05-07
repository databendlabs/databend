---
title: toStartOfISOYear
---

Returns the first day of the ISO year for a date or a date with time.

## Syntax

```sql
toStartOfISOYear( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date16/date32/datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT toStartOfISOYear(to_date(18869));
+---------------------------------+
| toStartOfISOYear(to_date(18869)) |
+---------------------------------+
| 2021-01-04                      |
+---------------------------------+

SELECT toStartOfISOYear(to_datetime(1630812366));
+------------------------------------------+
| toStartOfISOYear(to_datetime(1630812366)) |
+------------------------------------------+
| 2021-01-04                               |
+------------------------------------------+
```
