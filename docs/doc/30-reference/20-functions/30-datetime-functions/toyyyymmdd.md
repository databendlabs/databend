---
title: TO_YYYYMMDD
---

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY * 10000 + MM * 100 + DD).
## Syntax

```sql
to_yyyymmdd( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

UInt32, returns in `YYYYMMDD` format.

## Examples

```sql
SELECT to_date(18875);
+---------------+
| to_date(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT to_yyyymmdd(to_date(18875));
+---------------------------+
| to_yyyymmdd(to_date(18875)) |
+---------------------------+
|                  20210905 |
+---------------------------+

SELECT to_datetime(1630833797);
+------------------------+
| to_datetime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

SELECT to_yyyymmdd(to_datetime(1630833797));
+------------------------------------+
| to_yyyymmdd(to_datetime(1630833797)) |
+------------------------------------+
|                           20210905 |
+------------------------------------+
```
