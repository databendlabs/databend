---
title: TO_DAY_OF_MONTH
---

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

## Syntax

```sql
to_day_of_month( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
SELECT to_day_of_month(to_date(18869));
+-----------------------------+
| to_day_of_month(to_date(18869)) |
+-----------------------------+
|                          30 |
+-----------------------------+

SELECT to_day_of_month(to_datetime(1630812366));
+--------------------------------------+
| to_day_of_month(to_datetime(1630812366)) |
+--------------------------------------+
|                                    5 |
+--------------------------------------+
```
