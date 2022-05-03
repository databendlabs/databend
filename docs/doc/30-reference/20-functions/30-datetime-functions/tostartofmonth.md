---
title: toStartOfMonth
---

Rounds down a date or date with time to the first day of the month.
Returns the date.

## Syntax

```sql
toStartOfMonth( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT toStartOfMonth(to_date(18869));
+-------------------------------+
| toStartOfMonth(to_date(18869)) |
+-------------------------------+
| 2021-08-01                    |
+-------------------------------+

SELECT toStartOfMonth(to_datetime(1630812366));
+----------------------------------------+
| toStartOfMonth(to_datetime(1630812366)) |
+----------------------------------------+
| 2021-09-01                             |
+----------------------------------------+
```
