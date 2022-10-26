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
| `<expr>`    | timestamp   |

## Return Type
Datetime object, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT time_slot(now());
+----------------------------+
| time_slot(now())           |
+----------------------------+
| 2022-10-15 02:30:00.000000 |
+----------------------------+

SELECT time_slot(to_timestamp(1630812366));
+-------------------------------------+
| time_slot(to_timestamp(1630812366)) |
+-------------------------------------+
| 2021-09-05 03:00:00.000000          |
+-------------------------------------+
```
