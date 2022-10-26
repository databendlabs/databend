---
title: TO_START_OF_TEN_MINUTES
---

Rounds down a date with time (timestamp/datetime) to the start of the ten-minute interval.

## Syntax

```sql
to_start_of_ten_minutes(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | timestamp |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT to_start_of_ten_minutes(now());
+--------------------------------+
| to_start_of_ten_minutes(now()) |
+--------------------------------+
| 2022-10-15 03:20:00.000000     |
+--------------------------------+

SELECT to_start_of_ten_minutes(to_timestamp(1630812366));
+---------------------------------------------------+
| to_start_of_ten_minutes(to_timestamp(1630812366)) |
+---------------------------------------------------+
| 2021-09-05 03:20:00.000000                        |
+---------------------------------------------------+
```
