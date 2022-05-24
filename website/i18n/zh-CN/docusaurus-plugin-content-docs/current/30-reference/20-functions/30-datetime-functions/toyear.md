---
title: TOYEAR
---

Converts a date or date with time to a UInt16 number containing the year number (AD).

## Syntax

```sql
TOYEAR( <expr> )
```

## Arguments

| Arguments      | Description   |
| -------------- | ------------- |
| `<expr>` | date/datetime |

## Return Type

A `UInt16` date type value

## Examples

```sql
SELECT toyear(now());
+---------------+
| toyear(now()) |
+---------------+
|          2022 |
+---------------+

SELECT toyear(to_datetime(1));
+-----------------------+
| toyear(to_datetime(1)) |
+-----------------------+
|                  1970 |
+-----------------------+
```
