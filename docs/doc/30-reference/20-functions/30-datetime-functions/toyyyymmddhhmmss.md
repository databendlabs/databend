---
title: TO_YYYYMMDDHHMMSS
---

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).
## Syntax

```sql
to_yyyymmddhhmmss( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

UInt64, returns in `YYYYMMDDhhmmss` format.

## Examples

```sql
SELECT to_date(18875);
+---------------+
| to_date(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT to_yyyymmddhhmmss(to_date(18875));
+---------------------------------+
| to_yyyymmddhhmmss(to_date(18875)) |
+---------------------------------+
|                  20210905000000 |
+---------------------------------+

SELECT to_datetime(1630833797);
+------------------------+
| to_datetime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

SELECT to_yyyymmddhhmmss(to_datetime(1630833797));
+------------------------------------------+
| to_yyyymmddhhmmss(to_datetime(1630833797)) |
+------------------------------------------+
|                           20210905092317 |
+------------------------------------------+
```
