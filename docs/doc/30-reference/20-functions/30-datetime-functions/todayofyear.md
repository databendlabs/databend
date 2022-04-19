---
title: toDayOfYear
---

Converts a date or date with time to a UInt16 number containing the number of the day of the year (1-366).

## Syntax

```sql
toDayOfYear(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
A `UInt16` number datatype.

## Examples

```sql
SELECT toDayOfYear(toDate(18869));
+----------------------------+
| toDayOfYear(toDate(18869)) |
+----------------------------+
|                        242 |
+----------------------------+

SELECT toDayOfYear(now());
+--------------------+
| toDayOfYear(now()) |
+--------------------+
|                 88 |
+--------------------+
```
