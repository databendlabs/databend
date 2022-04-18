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
SELECT toDate(18875);
+---------------+
| toDate(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT toYYYYMMDDhhmmss(toDate(18875));
+---------------------------------+
| toYYYYMMDDhhmmss(toDate(18875)) |
+---------------------------------+
|                  20210905000000 |
+---------------------------------+

SELECT typeof(toYYYYMMDDhhmmss(toDate(18875)));
+---------------------------------------------+
| typeof(toYYYYMMDDhhmmss(toDate(18875)))     |
+---------------------------------------------+
| UInt64                                      |
+---------------------------------------------+

SELECT toDateTime(1630833797);
+------------------------+
| toDateTime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

SELECT toYYYYMMDDhhmmss(toDateTime(1630833797));
+------------------------------------------+
| toYYYYMMDDhhmmss(toDateTime(1630833797)) |
+------------------------------------------+
|                           20210905092317 |
+------------------------------------------+

SELECT Typeof(toYYYYMMDDhhmmss(toDateTime(1630833797)));
+------------------------------------------------------+
| typeof(toYYYYMMDDhhmmss(toDateTime(1630833797)))     |
+------------------------------------------------------+
| UInt64                                               |
+------------------------------------------------------+
```
