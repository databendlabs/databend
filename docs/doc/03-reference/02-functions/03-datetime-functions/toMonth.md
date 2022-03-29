---
title: toMonth
---

Converts a date or date with time to a UInt8 number containing the month number (1-12).

## Syntax

```sql
toMonth(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
A `UInt8` number datatype.

## Examples

```sql
mysql> select toMonth(toDate(18869));
+------------------------+
| toMonth(toDate(18869)) |
+------------------------+
|                      8 |
+------------------------+

mysql>  select toMonth(toDateTime(1630812366));
+---------------------------------+
| toMonth(toDateTime(1630812366)) |
+---------------------------------+
|                               9 |
+---------------------------------+
```
