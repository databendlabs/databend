---
title: DATE_SUB
---

Subtracts the time interval or date interval from the provided date or date with time.

## Syntax

```sql
DATE_SUB(<unit>, <value>,  <date_or_time_expr>)
```
## Arguments

| Arguments             | Description |
|-----------------------| ----------- |
| `<unit>`              | Must be of the following values: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE` and `SECOND` |
| `<value>`             |This is the number of units of time that you want to add. For example, if you want to add 2 days, this will be 2.|
| `<date_or_time_expr>` | A value of `DATE`, `TIME`, or `TIMESTAMP` type |

## Return Type

The function returns a value of the same type as the `<date_or_time_expr>` argument.

## Examples

Query:
```sql
SELECT date_sub(YEAR, 1, to_date('2018-01-02'));
```

Result:
```
+---------------------------------------------------+
| DATE_SUB(YEAR, INTERVAL 1, to_date('2018-01-02')) |
+---------------------------------------------------+
| 2017-01-02                                        |
+---------------------------------------------------+
```
