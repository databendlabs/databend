---
title: toStartOfFiveMinutes
---

Rounds down a date with time to the start of the five-minute interval.
## Syntax

```sql
toStartOfFiveMinutes( <expr> )
```

## Arguments

| Arguments      | Description |
| -------------- | ----------- |
| `<expr>` | datetime    |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfFiveMinutes(now());
+-----------------------------+
| toStartOfFiveMinutes(now()) |
+-----------------------------+
| 2022-03-29 06:45:00         |
+-----------------------------+

SELECT toStartOfFiveMinutes(to_datetime(1630812366));
+----------------------------------------------+
| toStartOfFiveMinutes(to_datetime(1630812366)) |
+----------------------------------------------+
| 2021-09-05 03:25:00                          |
+----------------------------------------------+
```
