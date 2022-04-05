---
title: TOYEAR
---

Converts a date or date with time to a UInt16 number containing the year number (AD).

## Syntax

```sql
TOYEAR(date)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| date | date16/date32/datetime |

## Return Type

A `UInt16` date type value

## Examples

```sql
mysql> select toyear(now());
+---------------+
| toyear(now()) |
+---------------+
|          2022 |
+---------------+

mysql> select toyear(todatetime(1));
+-----------------------+
| toyear(todatetime(1)) |
+-----------------------+
|                  1970 |
+-----------------------+
```
