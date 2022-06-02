---
title: TODAYOFMONTH
---

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

## Syntax

```sql
toDayOfMonth( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
SELECT toDayOfMonth(to_date(18869));
+-----------------------------+
| toDayOfMonth(to_date(18869)) |
+-----------------------------+
|                          30 |
+-----------------------------+

SELECT toDayOfMonth(to_datetime(1630812366));
+--------------------------------------+
| toDayOfMonth(to_datetime(1630812366)) |
+--------------------------------------+
|                                    5 |
+--------------------------------------+
```
