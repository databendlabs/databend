---
title: toSecond
---

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).

## Syntax

```sql
toSecond(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
SELECT toSecond(now());
+-----------------+
| toSecond(now()) |
+-----------------+
|              14 |
+-----------------+

SELECT toSecond(to_datetime(1630812366));
+----------------------------------+
| toSecond(to_datetime(1630812366)) |
+----------------------------------+
|                                6 |
+----------------------------------+
```
