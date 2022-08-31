---
title: TO_MINUTE
---

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

## Syntax

```sql
to_minute( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
|  `<expr>` | datetime |

## Return Type

 `UInt8` datatype.

## Examples

```sql
SELECT to_minute(now());
+-----------------+
| to_minute(now()) |
+-----------------+
|              17 |
+-----------------+

SELECT to_minute(to_datetime(1630812366));
+----------------------------------+
| to_minute(to_datetime(1630812366)) |
+----------------------------------+
|                               26 |
+----------------------------------+
```
