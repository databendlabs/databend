---
title: TODAYOFMONTH
---

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

## Syntax

```sql
toDayOfMonth(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date32/date64/datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
mysql> select toDayOfMonth(toDate(18869));
+-----------------------------+
| toDayOfMonth(toDate(18869)) |
+-----------------------------+
|                          30 |
+-----------------------------+

mysql> select toDayOfMonth(toDateTime(1630812366));
+--------------------------------------+
| toDayOfMonth(toDateTime(1630812366)) |
+--------------------------------------+
|                                    5 |
+--------------------------------------+
```
