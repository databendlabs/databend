---
title: TO_YEAR
---

Converts a date or date with time to a UInt16 number containing the year number (AD).

## Syntax

```sql
to_year( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

A `UInt16` date type value

## Examples

```sql
SELECT to_year(now());
+---------------+
| to_year(now()) |
+---------------+
|          2022 |
+---------------+

SELECT to_year(to_datetime(1));
+-----------------------+
| to_year(to_datetime(1)) |
+-----------------------+
|                  1970 |
+-----------------------+
```
