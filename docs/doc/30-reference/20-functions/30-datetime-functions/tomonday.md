---
title: TO_MONDAY
---

Rounds down a date or date with time (timestamp/datetime) to the nearest Monday.
Returns the date.

## Syntax

```sql
to_monday( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/timestamp |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```sql
SELECT to_monday(now());
+---------------------------+
| to_date(to_monday(now())) |
+---------------------------+
| 2022-03-28                |
+---------------------------+

SELECT to_monday(to_timestamp(1630812366));
+-------------------------------------+
| to_monday(to_timestamp(1630812366)) |
+-------------------------------------+
| 2021-08-30                          |
+-------------------------------------+
```
