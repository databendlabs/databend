---
title: TO_DAY_OF_YEAR
---

Converts a date or date with time to a UInt16 number containing the number of the day of the year (1-366).

## Syntax

```sql
to_day_of_year( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

A `UInt16` number datatype.

## Examples

```sql
SELECT to_day_of_year(to_date(18869));
+----------------------------+
| to_day_of_year(to_date(18869)) |
+----------------------------+
|                        242 |
+----------------------------+

SELECT to_day_of_year(now());
+--------------------+
| to_day_of_year(now()) |
+--------------------+
|                 88 |
+--------------------+
```
