---
title: toStartOfYear
---

Returns the first day of the year for a date or a date with time.

## Syntax

```sql
toStartOfYear( <expr> )
```

## Arguments

| Arguments      | Description   |
| -------------- | ------------- |
| `<expr>` | date/datetime |

## Return Type

Datetime object, returns date in “YYYY-MM-DD” format.

## Examples

```
SELECT toStartOfYear(to_date(18869));
+------------------------------+
| toStartOfYear(to_date(18869)) |
+------------------------------+
| 2021-01-01                   |
+------------------------------+

SELECT toStartOfYear(to_datetime(1630812366));
+---------------------------------------+
| toStartOfYear(to_datetime(1630812366)) |
+---------------------------------------+
| 2021-01-01                            |
+---------------------------------------+
```
