---
title: toStartOfHour
---

Rounds down a date with time to the start of the hour.
## Syntax

```sql
toStartOfHour( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT toStartOfHour(now());
+----------------------+
| toStartOfHour(now()) |
+----------------------+
| 2022-03-29 06:00:00  |
+----------------------+

SELECT toStartOfHour(to_datetime(1630812366));
+---------------------------------------+
| toStartOfHour(to_datetime(1630812366)) |
+---------------------------------------+
| 2021-09-05 03:00:00                   |
+---------------------------------------+
```
