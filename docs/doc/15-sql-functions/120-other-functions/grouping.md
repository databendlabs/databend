---
title: GROUPING 
---

Returns a bit mask indicating which `GROUP BY` expressions are not included in the current grouping set. Bits are assigned with the rightmost argument corresponding to the least-significant bit; each bit is 0 if the corresponding expression is included in the grouping criteria of the grouping set generating the current result row, and 1 if it is not included.

## Syntax

```sql
GROUPING ( expr [, expr, ...] )
```

:::note
`GROUPING` can only be used with `GROUPING SETS`, `ROLLUP`, or `CUBE`, and its arguments must be in the grouping sets list.
:::

## Arguments

Grouping sets items.

## Return Type

UInt32.

## Examples

```sql
select a, b, grouping(a), grouping(b), grouping(a,b), grouping(b,a) from t group by grouping sets ((a,b),(a),(b), ()) ;
+------+------+-------------+-------------+----------------+----------------+
| a    | b    | grouping(a) | grouping(b) | grouping(a, b) | grouping(b, a) |
+------+------+-------------+-------------+----------------+----------------+
| NULL | A    |           1 |           0 |              2 |              1 |
| a    | NULL |           0 |           1 |              1 |              2 |
| b    | A    |           0 |           0 |              0 |              0 |
| NULL | NULL |           1 |           1 |              3 |              3 |
| a    | A    |           0 |           0 |              0 |              0 |
| b    | B    |           0 |           0 |              0 |              0 |
| b    | NULL |           0 |           1 |              1 |              2 |
| a    | B    |           0 |           0 |              0 |              0 |
| NULL | B    |           1 |           0 |              2 |              1 |
+------+------+-------------+-------------+----------------+----------------+
```