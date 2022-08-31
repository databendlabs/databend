---
title: TO_START_OF_ISO_YEAR
---

Returns the first day of the ISO year for a date or a date with time.

## Syntax

```sql
to_start_of_iso_year( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date16/date32/datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT to_start_of_iso_year(to_date(18869));
+---------------------------------+
| to_start_of_iso_year(to_date(18869)) |
+---------------------------------+
| 2021-01-04                      |
+---------------------------------+

SELECT to_start_of_iso_year(to_datetime(1630812366));
+------------------------------------------+
| to_start_of_iso_year(to_datetime(1630812366)) |
+------------------------------------------+
| 2021-01-04                               |
+------------------------------------------+
```
