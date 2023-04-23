---
title: TO_MONTH
---

Convert a date or date with time (timestamp/datetime) to a UInt8 number containing the month number (1-12).

## Syntax

```sql
TO_MONTH(<expr>)
```

## Arguments

| Arguments | Description    |
|-----------|----------------|
| `<expr>`  | date/timestamp |

## Return Type

 `TINYINT`

## Examples

```sql
SELECT to_month(to_date(18869));
+--------------------------+
| to_month(to_date(18869)) |
+--------------------------+
|                        8 |
+--------------------------+

 SELECT to_month(to_timestamp(1630812366));
+------------------------------------+
| to_month(to_timestamp(1630812366)) |
+------------------------------------+
|                                  9 |
+------------------------------------+
```
