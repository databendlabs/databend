---
title: toMonday
---

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## Syntax

```sql
toMonday(expr)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expr | date16/date32/datetime |

## Return Type
`UInt16` datatype.

## Examples

```sql
SELECT toMonday(now());
+-----------------+
| tomonday(now()) |
+-----------------+
|           19079 |
+-----------------+

SELECT todate(toMonday(now()));
+-------------------------+
| todate(toMonday(now())) |
+-------------------------+
| 2022-03-28              |
+-------------------------+

SELECT toMonday(toDateTime(1630812366));
+----------------------------------+
| toMonday(toDateTime(1630812366)) |
+----------------------------------+
|                            18869 |
+----------------------------------+

```
