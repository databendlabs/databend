---
title: toYYYYMM
---

Converts a date or date with time to a UInt32 number containing the year and month number.

## Syntax

```sql
toYYYYMM(expr)
```

## Return Type

UInt32, returns in `YYYYMM` format.

## Examples

```sql
SELECT toDate(18875);
+---------------+
| toDate(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT toYYYYMM(toDate(18875));
+-------------------------+
| toYYYYMM(toDate(18875)) |
+-------------------------+
|                  202109 |
+-------------------------+

SELECT typeof(toYYYYMM(toDate(18875)));
+-------------------------------------+
| typeof(toYYYYMM(toDate(18875)))     |
+-------------------------------------+
| UInt32                              |
+-------------------------------------+
```
