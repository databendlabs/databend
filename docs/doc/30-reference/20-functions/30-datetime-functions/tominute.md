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
SELECT toMinute(now());
+-----------------+
| toMinute(now()) |
+-----------------+
|              17 |
+-----------------+

SELECT toMinute(to_datetime(1630812366));
+----------------------------------+
| toMinute(to_datetime(1630812366)) |
+----------------------------------+
|                               26 |
+----------------------------------+
```
