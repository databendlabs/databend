---
title: TO_YYYYMMDDHHMMSS
---

Converts a date or date with time (timestamp/datetime) to a UInt64 number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).
## Syntax

```sql
to_yyyymmddhhmmss( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/timestamp |

## Return Type

UInt64, returns in `YYYYMMDDhhmmss` format.

## Examples

```sql
SELECT to_date(18875), to_yyyymmddhhmmss(to_date(18875));
+----------------+-----------------------------------+
| to_date(18875) | to_yyyymmddhhmmss(to_date(18875)) |
+----------------+-----------------------------------+
| 2021-09-05     |                    20210905000000 |
+----------------+-----------------------------------+

SELECT to_timestamp(1630833797), to_yyyymmddhhmmss(to_timestamp(1630833797));
+----------------------------+---------------------------------------------+
| to_timestamp(1630833797)   | to_yyyymmddhhmmss(to_timestamp(1630833797)) |
+----------------------------+---------------------------------------------+
| 2021-09-05 09:23:17.000000 |                              20210905092317 |
+----------------------------+---------------------------------------------+
```
