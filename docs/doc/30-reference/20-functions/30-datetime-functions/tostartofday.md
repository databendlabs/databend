---
title: TO_START_OF_DAY
---

Rounds down a date with time to the start of the day.
## Syntax

```sql
to_start_of_day( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT to_start_of_day(now());
+---------------------+
| to_start_of_day(now()) |
+---------------------+
| 2022-03-29 00:00:00 |
+---------------------+

SELECT to_start_of_day(to_datetime(1630812366));
+--------------------------------------+
| to_start_of_day(to_datetime(1630812366)) |
+--------------------------------------+
| 2021-09-05 00:00:00                  |
+--------------------------------------+
```
