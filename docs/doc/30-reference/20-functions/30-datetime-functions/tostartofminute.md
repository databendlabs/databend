---
title: TO_START_OF_MINUTE
---

Rounds down a date with time to the start of the minute.

## Syntax

```sql
to_start_of_minute( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT to_start_of_minute(now());
+------------------------+
| to_start_of_minute(now()) |
+------------------------+
| 2022-03-29 06:43:00    |
+------------------------+

SELECT to_start_of_minute(to_datetime(1630812366));
+-----------------------------------------+
| to_start_of_minute(to_datetime(1630812366)) |
+-----------------------------------------+
| 2021-09-05 03:26:00                     |
+-----------------------------------------+
```
