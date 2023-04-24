---
title: TO_DAY_OF_YEAR
---

Converts a date or date with time (timestamp/datetime) to a UInt16 number containing the number of the day of the year (1-366).

## Syntax

```sql
TO_DAY_OF_YEAR(<expr>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/timestamp |

## Return Type

`SMALLINT`

## Examples

```sql
SELECT to_day_of_year(to_date(18869));
+--------------------------------+
| to_day_of_year(to_date(18869)) |
+--------------------------------+
|                            242 |
+--------------------------------+

SELECT to_day_of_year(now());
+-----------------------+
| to_day_of_year(now()) |
+-----------------------+
|                    88 |
+-----------------------+
```
