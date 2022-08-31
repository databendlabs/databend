---
title: TO_SECOND
---

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).

## Syntax

```sql
to_second( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | datetime |

## Return Type

`UInt8` datatype.

## Examples

```sql
SELECT to_second(now());
+-----------------+
| to_second(now()) |
+-----------------+
|              14 |
+-----------------+

SELECT to_second(to_datetime(1630812366));
+----------------------------------+
| to_second(to_datetime(1630812366)) |
+----------------------------------+
|                                6 |
+----------------------------------+
```
