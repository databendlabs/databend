---
title: toYYYYMMDDhhmmss
---

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).
## Syntax

```sql
toYYYYMMDDhhmmss(expr)
```

## Return Type

UInt64, returns in `YYYYMMDDhhmmss` format.

## Examples

```sql
mysql> select toDate(18875);
+---------------+
| toDate(18875) |
+---------------+
| 2021-09-05    |
+---------------+

mysql> select toYYYYMMDDhhmmss(toDate(18875));
+---------------------------------+
| toYYYYMMDDhhmmss(toDate(18875)) |
+---------------------------------+
|                  20210905000000 |
+---------------------------------+

mysql> select toTypeName(toYYYYMMDDhhmmss(toDate(18875)));
+---------------------------------------------+
| toTypeName(toYYYYMMDDhhmmss(toDate(18875))) |
+---------------------------------------------+
| UInt64                                      |
+---------------------------------------------+

mysql> select toDateTime(1630833797);
+------------------------+
| toDateTime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

mysql> select toYYYYMMDDhhmmss(toDateTime(1630833797));
+------------------------------------------+
| toYYYYMMDDhhmmss(toDateTime(1630833797)) |
+------------------------------------------+
|                           20210905092317 |
+------------------------------------------+

mysql> select toTypeName(toYYYYMMDDhhmmss(toDateTime(1630833797)));
+------------------------------------------------------+
| toTypeName(toYYYYMMDDhhmmss(toDateTime(1630833797))) |
+------------------------------------------------------+
| UInt64                                               |
+------------------------------------------------------+
```
