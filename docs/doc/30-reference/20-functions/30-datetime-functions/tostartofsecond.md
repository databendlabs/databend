---
title: TO_START_OF_SECOND
---

Rounds down a date with time to the start of the second.

## Syntax

```sql
to_start_of_second(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT to_start_of_second(now());;
+------------------------+
| to_start_of_second(now()) |
+------------------------+
| 2022-04-13 13:53:47    |
+------------------------+
```
