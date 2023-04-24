---
title: TO_START_OF_MINUTE
---

Rounds down a date with time (timestamp/datetime) to the start of the minute.

## Syntax

```sql
TO_START_OF_MINUTE( <expr> )
```

## Arguments

| Arguments | Description |
|-----------|-------------|
| `<expr>`  | timestamp   |

## Return Type

`TIMESTAMP`, returns date in “YYYY-MM-DD hh:mm:ss.ffffff” format.

## Examples

```sql
SELECT to_start_of_minute(now());
+----------------------------+
| to_start_of_minute(now())  |
+----------------------------+
| 2022-10-15 03:18:00.000000 |
+----------------------------+

SELECT to_start_of_minute(to_timestamp(1630812366));
+----------------------------------------------+
| to_start_of_minute(to_timestamp(1630812366)) |
+----------------------------------------------+
| 2021-09-05 03:26:00.000000                   |
+----------------------------------------------+
```
