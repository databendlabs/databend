---
title: TO_START_OF_HOUR
---

Rounds down a date with time (timestamp/datetime) to the start of the hour.
## Syntax

```sql
to_start_of_hour( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | timestamp |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT to_start_of_hour(now());
+----------------------------+
| to_start_of_hour(now())    |
+----------------------------+
| 2022-10-15 03:00:00.000000 |
+----------------------------+

SELECT to_start_of_hour(to_timestamp(1630812366));
+--------------------------------------------+
| to_start_of_hour(to_timestamp(1630812366)) |
+--------------------------------------------+
| 2021-09-05 03:00:00.000000                 |
+--------------------------------------------+
```
