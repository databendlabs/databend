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
 `UInt8` datatype.

## Examples

```sql
SELECT toMonth(toDate(18869));
+------------------------+
| toMonth(toDate(18869)) |
+------------------------+
|                      8 |
+------------------------+

 SELECT toMonth(toDateTime(1630812366));
+---------------------------------+
| toMonth(toDateTime(1630812366)) |
+---------------------------------+
|                               9 |
+---------------------------------+
```
