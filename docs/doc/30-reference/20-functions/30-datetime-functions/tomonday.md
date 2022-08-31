---
title: TO_MONDAY
---

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## Syntax

```sql
to_monday( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

`UInt16` datatype.

## Examples

```sql
SELECT to_monday(now());
+-----------------+
| to_monday(now()) |
+-----------------+
|           19079 |
+-----------------+

SELECT to_date(to_monday(now()));
+-------------------------+
| to_date(to_monday(now())) |
+-------------------------+
| 2022-03-28              |
+-------------------------+

SELECT to_monday(to_datetime(1630812366));
+----------------------------------+
| to_monday(to_datetime(1630812366)) |
+----------------------------------+
|                            18869 |
+----------------------------------+

```
