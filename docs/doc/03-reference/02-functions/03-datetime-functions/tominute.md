---
title: toMinute
---

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

## Syntax

```sql
toMinute(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
 `UInt8` datatype.

## Examples

```sql
mysql> select toMinute(now());
+-----------------+
| toMinute(now()) |
+-----------------+
|              17 |
+-----------------+

mysql> select toMinute(toDateTime(1630812366));
+----------------------------------+
| toMinute(toDateTime(1630812366)) |
+----------------------------------+
|                               26 |
+----------------------------------+
```
