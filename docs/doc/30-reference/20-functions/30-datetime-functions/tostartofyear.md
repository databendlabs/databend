---
title: TO_START_OF_YEAR
---

Returns the first day of the year for a date or a date with time.

## Syntax

```sql
to_start_of_year( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
SELECT to_start_of_year(to_date(18869));
+------------------------------+
| to_start_of_year(to_date(18869)) |
+------------------------------+
| 2021-01-01                   |
+------------------------------+

SELECT to_start_of_year(to_datetime(1630812366));
+---------------------------------------+
| to_start_of_year(to_datetime(1630812366)) |
+---------------------------------------+
| 2021-01-01                            |
+---------------------------------------+
```
