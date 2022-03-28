---
title: toYYYYMMDD
---

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY * 10000 + MM * 100 + DD).
## Syntax

```sql
toYYYYMMDD(expr)
```

## Return Type

UInt32, returns in `YYYYMMDD` format.

## Examples

```sql
mysql> select toDate(18875);
+---------------+
| toDate(18875) |
+---------------+
| 2021-09-05    |
+---------------+

mysql> select toYYYYMMDD(toDate(18875));
+---------------------------+
| toYYYYMMDD(toDate(18875)) |
+---------------------------+
|                  20210905 |
+---------------------------+

mysql> select toTypeName(toYYYYMMDD(toDate(18875)));
+---------------------------------------+
| toTypeName(toYYYYMMDD(toDate(18875))) |
+---------------------------------------+
| UInt32                                |
+---------------------------------------+

mysql> select toDateTime(1630833797);
+------------------------+
| toDateTime(1630833797) |
+------------------------+
| 2021-09-05 09:23:17    |
+------------------------+

mysql> select toYYYYMMDD(toDateTime(1630833797));
+------------------------------------+
| toYYYYMMDD(toDateTime(1630833797)) |
+------------------------------------+
|                           20210905 |
+------------------------------------+

mysql> select toTypeName(toYYYYMMDD(toDateTime(1630833797)));
+------------------------------------------------+
| toTypeName(toYYYYMMDD(toDateTime(1630833797))) |
+------------------------------------------------+
| UInt32                                         |
+------------------------------------------------+
```
