---
title: TO_START_OF_MONTH
---

Rounds down a date or date with time to the first day of the month.
Returns the date.

## Syntax

```sql
to_start_of_month( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT to_start_of_month(to_date(18869));
+-------------------------------+
| to_start_of_month(to_date(18869)) |
+-------------------------------+
| 2021-08-01                    |
+-------------------------------+

SELECT to_start_of_month(to_datetime(1630812366));
+----------------------------------------+
| to_start_of_month(to_datetime(1630812366)) |
+----------------------------------------+
| 2021-09-01                             |
+----------------------------------------+
```
