---
title: TO_START_OF_QUARTER
---

Rounds down a date or date with time to the first day of the quarter.
The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

## Syntax

```sql
to_start_of_quarter(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date/datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT to_start_of_quarter(to_date(18869));
+---------------------------------+
| to_start_of_quarter(to_date(18869)) |
+---------------------------------+
| 2021-07-01                      |
+---------------------------------+

SELECT to_start_of_quarter(to_datetime(1630812366));
+------------------------------------------+
| to_start_of_quarter(to_datetime(1630812366)) |
+------------------------------------------+
| 2021-07-01                               |
+------------------------------------------+
```
