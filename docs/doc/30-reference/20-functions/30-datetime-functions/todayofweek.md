---
title: TO_DAY_OF_WEEK
---

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

## Syntax

```sql
to_day_of_week( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
SELECT to_day_of_week(to_date(18869));
+----------------------------+
| to_day_of_week(to_date(18869)) |
+----------------------------+
|                          1 |
+----------------------------+

SELECT to_day_of_week(now());
+--------------------+
| to_day_of_week(now()) |
+--------------------+
|                  2 |
+--------------------+
```
