---
title: GROUPING SETS
---

More complex grouping operations than those described above are possible using the concept of grouping sets. The data is grouped separately by each specified grouping set, aggregates computed for each group just as for simple GROUP BY clauses. The final result is the union of each separate result.

Each sublist of `GROUPING SETS` may specify zero or more columns or expressions and is interpreted the same way as though it were directly in the `GROUP BY` clause. An empty grouping set means that all rows are aggregated down to a single group (which is output even if no input rows were present), as described above for the case of aggregate functions with no GROUP BY clause.

References to the grouping columns or expressions are replaced by null values in result rows for grouping sets in which those columns do not appear. To distinguish which grouping a particular output row resulted from, the [GROUPING function](../../15-sql-functions/120-other-functions/grouping.md) can be used.

## Syntax

```sql
SELECT ...
GROUP BY 
GROUPING SETS ( grouping_sets, [ grouping_sets, ...] )
```

```
grouping_sets := ( expr [, expr, ...] )
```

## `CUBE` and `ROLLUP`

Two syntactic sugar forms of `GROUPING SETS` are available: `CUBE` and `ROLLUP`. 

```sql
SELECT ...
GROUP BY CUBE ( expr [, expr, ...] );

SELECT ...
GROUP BY ROLLUP ( expr [, expr, ...] );
```

`CUBE` represents the given list and all of its possible subsets (i.e., the power set). For example, `CUBE (a, b, c)` will be desugared to:

```sql
GROUPING SETS (
    ( a, b, c ),
    ( a, b    ),
    ( a,    c ),
    ( a       ),
    (    b, c ),
    (    b    ),
    (       c ),
    (         )
)
```

`ROLLUP ` represents the given list of expressions and all prefixes of the list including the empty list. For example `ROLLUP (a, b, c)` will be desugared to:

```sql
GROUPING SETS (
    ( a, b, c ),
    ( a, b    ),
    ( a       ),
    (         )
)
```

## Examples

```sql
SELECT number%2 as c1, number%3 as c2, MAX(number) FROM numbers(10) GROUP BY GROUPING SETS ((c1, c2), (c1), (c2), ());
+------+------+-------------+
| c1   | c2   | max(number) |
+------+------+-------------+
|    0 |    0 |           6 |
|    0 | NULL |           8 |
|    1 |    0 |           9 |
| NULL | NULL |           9 |
|    1 |    2 |           5 |
|    1 | NULL |           9 |
| NULL |    0 |           9 |
| NULL |    1 |           7 |
| NULL |    2 |           8 |
|    1 |    1 |           7 |
|    0 |    2 |           8 |
|    0 |    1 |           4 |
+------+------+-------------+

SELECT number%2 as c1, number%3 as c2, MAX(number) FROM numbers(10) GROUP BY CUBE (c1, c2);
+------+------+-------------+
| c1   | c2   | max(number) |
+------+------+-------------+
|    0 |    0 |           6 |
|    0 | NULL |           8 |
|    1 |    0 |           9 |
| NULL | NULL |           9 |
|    1 |    2 |           5 |
|    1 | NULL |           9 |
| NULL |    0 |           9 |
| NULL |    1 |           7 |
| NULL |    2 |           8 |
|    1 |    1 |           7 |
|    0 |    2 |           8 |
|    0 |    1 |           4 |
+------+------+-------------+

SELECT number%2 as c1, number%3 as c2, MAX(number) FROM numbers(10) GROUP BY ROLLUP (c1, c2);
+------+------+-------------+
| c1   | c2   | max(number) |
+------+------+-------------+
|    0 |    0 |           6 |
|    0 | NULL |           8 |
|    1 |    0 |           9 |
| NULL | NULL |           9 |
|    1 |    2 |           5 |
|    1 | NULL |           9 |
|    1 |    1 |           7 |
|    0 |    2 |           8 |
|    0 |    1 |           4 |
+------+------+-------------+
```