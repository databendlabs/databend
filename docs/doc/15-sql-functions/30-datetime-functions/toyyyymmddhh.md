---
title: TO_YYYYMMDDHH
---

Formats a given date or timestamp into a string representation in the format "YYYYMMDDHH" (Year, Month, Day, Hour).

## Syntax

```sql
TO_YYYYMMDDHH(<expr>)
```

## Arguments

| Arguments | Description   |
|-----------|---------------|
| `<expr>`  | date/datetime |

## Return Type

Returns an unsigned 64-bit integer (UInt64) in the format "YYYYMMDDHH".

## Examples

```sql
SELECT to_yyyymmddhh(to_datetime(1630833797000000))
----
2021090509


SELECT to_yyyymmddhh(to_date(18875))
----
2021090500
```