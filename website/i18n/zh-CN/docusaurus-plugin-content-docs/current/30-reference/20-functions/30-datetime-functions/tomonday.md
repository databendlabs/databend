---
title: toMonday
---

Rounds down a date or date with time to the nearest Monday. Returns the date.

## Syntax

```sql
toMonday( <expr> )
```

## Arguments

| Arguments      | Description   |
| -------------- | ------------- |
| `<expr>` | date/datetime |

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

SELECT to_date(toMonday(now()));
+-------------------------+
| to_date(toMonday(now())) |
+-------------------------+
| 2022-03-28              |
+-------------------------+

SELECT toMonday(to_datetime(1630812366));
+----------------------------------+
| toMonday(to_datetime(1630812366)) |
+----------------------------------+
|                            18869 |
+----------------------------------+

```
