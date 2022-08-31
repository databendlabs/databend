---
title: TIME_SLOT
---

Rounds the time to the half hour.
## Syntax

```sql
time_slot( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss” format.

## Examples

```sql
SELECT time_slot(now());
+---------------------+
| time_slot(now())     |
+---------------------+
| 2022-03-29 06:30:00 |
+---------------------+

SELECT time_slot(to_datetime(1630812366));
+----------------------------------+
| time_slot(to_datetime(1630812366)) |
+----------------------------------+
| 2021-09-05 03:00:00              |
+----------------------------------+
```
