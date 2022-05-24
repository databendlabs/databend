---
title: toStartOfMinute
---

Rounds down a date with time to the start of the minute.

## Syntax

```sql
toStartOfMinute( <expr> )
```

## Arguments

| Arguments      | Description |
| -------------- | ----------- |
| `<expr>` | datetime    |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfMinute(now());
+------------------------+
| toStartOfMinute(now()) |
+------------------------+
| 2022-03-29 06:43:00    |
+------------------------+

SELECT toStartOfMinute(to_datetime(1630812366));
+-----------------------------------------+
| toStartOfMinute(to_datetime(1630812366)) |
+-----------------------------------------+
| 2021-09-05 03:26:00                     |
+-----------------------------------------+
```
