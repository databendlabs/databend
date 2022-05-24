---
title: toHour
---

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23). This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## Syntax

```sql
toHour( <expr> )
```

## Arguments

| Arguments      | Description |
| -------------- | ----------- |
| `<expr>` | datetime    |

## Return Type

 `UInt8` datatype.

## Examples

```sql
SELECT toHour(now());
+---------------+
| toHour(now()) |
+---------------+
|             6 |
+---------------+

SELECT toHour(to_datetime(1630812366));
+--------------------------------+
| toHour(to_datetime(1630812366)) |
+--------------------------------+
|                              3 |
+--------------------------------+
```
