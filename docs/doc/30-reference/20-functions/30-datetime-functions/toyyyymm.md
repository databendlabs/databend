---
title: TO_YYYYMM
---

Converts a date or date with time to a UInt32 number containing the year and month number.

## Syntax

```sql
to_yyyymm( <expr> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | date/datetime |

## Return Type

UInt32, returns in `YYYYMM` format.

## Examples

```sql
SELECT to_date(18875);
+---------------+
| to_date(18875) |
+---------------+
| 2021-09-05    |
+---------------+

SELECT to_yyyymm(to_date(18875));
+-------------------------+
| to_yyyymm(to_date(18875)) |
+-------------------------+
|                  202109 |
+-------------------------+
```
