---
title: TO_START_OF_DAY
---

Rounds down a date with time (timestamp/datetime) to the start of the day.
## Syntax

```sql
to_start_of_day( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | timestamp |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT to_start_of_day(now());
+----------------------------+
| to_start_of_day(now())     |
+----------------------------+
| 2022-10-15 00:00:00.000000 |
+----------------------------+

SELECT to_start_of_day(to_timestamp(1630812366));
+-------------------------------------------+
| to_start_of_day(to_timestamp(1630812366)) |
+-------------------------------------------+
| 2021-09-05 00:00:00.000000                |
+-------------------------------------------+
```
