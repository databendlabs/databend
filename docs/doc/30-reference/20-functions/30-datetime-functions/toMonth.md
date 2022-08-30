---
title: TO_MONTH
---

Converts a date or date with time to a UInt8 number containing the month number (1-12).

## Syntax

```sql
to_month( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

 `UInt8` datatype.

## Examples

```sql
SELECT to_month(to_date(18869));
+------------------------+
| to_month(to_date(18869)) |
+------------------------+
|                      8 |
+------------------------+

 SELECT to_month(to_datetime(1630812366));
+---------------------------------+
| to_month(to_datetime(1630812366)) |
+---------------------------------+
|                               9 |
+---------------------------------+
```
