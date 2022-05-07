---
title: toDayOfWeek
---

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

## Syntax

```sql
toDayOfWeek( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type
`UInt8` datatype.

## Examples

```sql
SELECT toDayOfWeek(to_date(18869));
+----------------------------+
| toDayOfWeek(to_date(18869)) |
+----------------------------+
|                          1 |
+----------------------------+

SELECT toDayOfWeek(now());
+--------------------+
| toDayOfWeek(now()) |
+--------------------+
|                  2 |
+--------------------+
```
