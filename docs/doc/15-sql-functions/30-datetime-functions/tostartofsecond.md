---
title: TO_START_OF_SECOND
---

Rounds down a date with time (timestamp/datetime) to the start of the second.

## Syntax

```sql
to_start_of_second(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | timestamp |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT to_start_of_second(now());
+----------------------------+
| to_start_of_second(now())  |
+----------------------------+
| 2022-10-15 03:20:55.000000 |
+----------------------------+
```
