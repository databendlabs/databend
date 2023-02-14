---
title: EXPLAIN ANALYZE
---

`EXPLAIN ANALYZE` used to display a query execution plan along with actual run-time performance statistics. 

This is useful for analyzing query performance and identifying bottlenecks in a query.

## Syntax

```sql
EXPLAIN ANALYZE <statement>
```

## Examples

TPC-H Q21:
```sql
EXPLAIN ANALYZE SELECT s_name,
    ->        Count(*) AS numwait
    -> FROM   supplier,
    ->        lineitem l1,
    ->        orders,
    ->        nation
    -> WHERE  s_suppkey = l1.l_suppkey
    ->        AND o_orderkey = l1.l_orderkey
    ->        AND o_orderstatus = 'F'
    ->        AND l1.l_receiptdate > l1.l_commitdate
    ->        AND EXISTS (SELECT *
    ->                    FROM   lineitem l2
    ->                    WHERE  l2.l_orderkey = l1.l_orderkey
    ->                           AND l2.l_suppkey <> l1.l_suppkey)
    ->        AND NOT EXISTS (SELECT *
    ->                        FROM   lineitem l3
    ->                        WHERE  l3.l_orderkey = l1.l_orderkey
    ->                               AND l3.l_suppkey <> l1.l_suppkey
    ->                               AND l3.l_receiptdate > l3.l_commitdate)
    ->        AND s_nationkey = n_nationkey
    ->        AND n_name = 'EGYPT'
    -> GROUP  BY s_name
    -> ORDER  BY numwait DESC,
    ->           s_name
    -> LIMIT  100;
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                          |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Limit                                                                                                                                                            |
| ├── limit: 100                                                                                                                                                   |
| ├── offset: 0                                                                                                                                                    |
| ├── estimated rows: 100.00                                                                                                                                       |
| ├── total process time: 0ms                                                                                                                                      |
| └── Sort                                                                                                                                                         |
|     ├── sort keys: [numwait DESC NULLS LAST, s_name ASC NULLS LAST]                                                                                              |
|     ├── estimated rows: 11000.00                                                                                                                                 |
|     ├── total process time: 0ms                                                                                                                                  |
|     └── EvalScalar                                                                                                                                               |
|         ├── expressions: [COUNT(*) (#70)]                                                                                                                        |
|         ├── estimated rows: 11000.00                                                                                                                             |
|         ├── total process time: 0ms                                                                                                                              |
|         └── AggregateFinal                                                                                                                                       |
|             ├── group by: [s_name]                                                                                                                               |
|             ├── aggregate functions: [count()]                                                                                                                   |
|             ├── estimated rows: 11000.00                                                                                                                         |
|             └── AggregatePartial                                                                                                                                 |
|                 ├── group by: [s_name]                                                                                                                           |
|                 ├── aggregate functions: [count()]                                                                                                               |
|                 ├── estimated rows: 11000.00                                                                                                                     |
|                 ├── total process time: 1ms                                                                                                                      |
|                 └── HashJoin                                                                                                                                     |
|                     ├── join type: LEFT ANTI                                                                                                                     |
|                     ├── build keys: [l3.l_orderkey (#52)]                                                                                                        |
|                     ├── probe keys: [l1.l_orderkey (#7)]                                                                                                         |
|                     ├── filters: [noteq(l3.l_suppkey (#54), l1.l_suppkey (#9))]                                                                                  |
|                     ├── estimated rows: 1633696.00                                                                                                               |
|                     ├── total process time: 788ms                                                                                                                |
|                     ├── Filter(Build)                                                                                                                            |
|                     │   ├── filters: [gt(l3.l_receiptdate (#64), l3.l_commitdate (#63))]                                                                         |
|                     │   ├── estimated rows: 2400786.33                                                                                                           |
|                     │   ├── total process time: 85ms                                                                                                             |
|                     │   └── TableScan                                                                                                                            |
|                     │       ├── table: default.tpch.lineitem                                                                                                     |
|                     │       ├── read rows: 7202359                                                                                                               |
|                     │       ├── read bytes: 42731029                                                                                                             |
|                     │       ├── partitions total: 9                                                                                                              |
|                     │       ├── partitions scanned: 9                                                                                                            |
|                     │       ├── pruning stats: [segments: <range pruning: 9 to 9>, blocks: <range pruning: 9 to 9, bloom pruning: 0 to 0>]                       |
|                     │       ├── push downs: [filters: [gt(l3.l_receiptdate (#64), l3.l_commitdate (#63))], limit: NONE]                                          |
|                     │       ├── output columns: [l_orderkey, l_suppkey, l_commitdate, l_receiptdate]                                                             |
|                     │       └── estimated rows: 7202359.00                                                                                                       |
|                     └── HashJoin(Probe)                                                                                                                          |
|                         ├── join type: LEFT SEMI                                                                                                                 |
|                         ├── build keys: [l2.l_orderkey (#36)]                                                                                                    |
|                         ├── probe keys: [l1.l_orderkey (#7)]                                                                                                     |
|                         ├── filters: [noteq(l2.l_suppkey (#38), l1.l_suppkey (#9))]                                                                              |
|                         ├── estimated rows: 1633696.00                                                                                                           |
|                         ├── total process time: 905ms                                                                                                            |
|                         ├── TableScan(Build)                                                                                                                     |
|                         │   ├── table: default.tpch.lineitem                                                                                                     |
|                         │   ├── read rows: 7202359                                                                                                               |
|                         │   ├── read bytes: 17507468                                                                                                             |
|                         │   ├── partitions total: 9                                                                                                              |
|                         │   ├── partitions scanned: 9                                                                                                            |
|                         │   ├── pruning stats: [segments: <range pruning: 9 to 9>, blocks: <range pruning: 9 to 9, bloom pruning: 0 to 0>]                       |
|                         │   ├── push downs: [filters: [], limit: NONE]                                                                                           |
|                         │   ├── output columns: [l_orderkey, l_suppkey]                                                                                          |
|                         │   └── estimated rows: 7202359.00                                                                                                       |
|                         └── HashJoin(Probe)                                                                                                                      |
|                             ├── join type: INNER                                                                                                                 |
|                             ├── build keys: [orders.o_orderkey (#23)]                                                                                            |
|                             ├── probe keys: [l1.l_orderkey (#7)]                                                                                                 |
|                             ├── filters: []                                                                                                                      |
|                             ├── estimated rows: 1633696.00                                                                                                       |
|                             ├── total process time: 338ms                                                                                                        |
|                             ├── Filter(Build)                                                                                                                    |
|                             │   ├── filters: [eq(orders.o_orderstatus (#25), "F")]                                                                               |
|                             │   ├── estimated rows: 550000.00                                                                                                    |
|                             │   ├── total process time: 42ms                                                                                                     |
|                             │   └── TableScan                                                                                                                    |
|                             │       ├── table: default.tpch.orders                                                                                               |
|                             │       ├── read rows: 1650000                                                                                                       |
|                             │       ├── read bytes: 5173599                                                                                                      |
|                             │       ├── partitions total: 3                                                                                                      |
|                             │       ├── partitions scanned: 3                                                                                                    |
|                             │       ├── pruning stats: [segments: <range pruning: 3 to 3>, blocks: <range pruning: 3 to 3, bloom pruning: 3 to 3>]               |
|                             │       ├── push downs: [filters: [eq(orders.o_orderstatus (#25), "F")], limit: NONE]                                                |
|                             │       ├── output columns: [o_orderkey, o_orderstatus]                                                                              |
|                             │       └── estimated rows: 1650000.00                                                                                               |
|                             └── HashJoin(Probe)                                                                                                                  |
|                                 ├── join type: INNER                                                                                                             |
|                                 ├── build keys: [nation.n_nationkey (#32)]                                                                                       |
|                                 ├── probe keys: [supplier.s_nationkey (#3)]                                                                                      |
|                                 ├── filters: []                                                                                                                  |
|                                 ├── estimated rows: 184766.67                                                                                                    |
|                                 ├── total process time: 93ms                                                                                                     |
|                                 ├── Filter(Build)                                                                                                                |
|                                 │   ├── filters: [eq(nation.n_name (#33), "EGYPT")]                                                                              |
|                                 │   ├── estimated rows: 16.67                                                                                                    |
|                                 │   ├── total process time: 0ms                                                                                                  |
|                                 │   └── TableScan                                                                                                                |
|                                 │       ├── table: default.tpch.nation                                                                                           |
|                                 │       ├── read rows: 50                                                                                                        |
|                                 │       ├── read bytes: 566                                                                                                      |
|                                 │       ├── partitions total: 2                                                                                                  |
|                                 │       ├── partitions scanned: 2                                                                                                |
|                                 │       ├── pruning stats: [segments: <range pruning: 2 to 2>, blocks: <range pruning: 2 to 2, bloom pruning: 2 to 2>]           |
|                                 │       ├── push downs: [filters: [eq(nation.n_name (#33), "EGYPT")], limit: NONE]                                               |
|                                 │       ├── output columns: [n_nationkey, n_name]                                                                                |
|                                 │       └── estimated rows: 50.00                                                                                                |
|                                 └── HashJoin(Probe)                                                                                                              |
|                                     ├── join type: INNER                                                                                                         |
|                                     ├── build keys: [supplier.s_suppkey (#0)]                                                                                    |
|                                     ├── probe keys: [l1.l_suppkey (#9)]                                                                                          |
|                                     ├── filters: []                                                                                                              |
|                                     ├── estimated rows: 11086.00                                                                                                 |
|                                     ├── total process time: 447ms                                                                                                |
|                                     ├── TableScan(Build)                                                                                                         |
|                                     │   ├── table: default.tpch.supplier                                                                                         |
|                                     │   ├── read rows: 11000                                                                                                     |
|                                     │   ├── read bytes: 42015                                                                                                    |
|                                     │   ├── partitions total: 2                                                                                                  |
|                                     │   ├── partitions scanned: 2                                                                                                |
|                                     │   ├── pruning stats: [segments: <range pruning: 2 to 2>, blocks: <range pruning: 2 to 2, bloom pruning: 0 to 0>]           |
|                                     │   ├── push downs: [filters: [], limit: NONE]                                                                               |
|                                     │   ├── output columns: [s_suppkey, s_name, s_nationkey]                                                                     |
|                                     │   └── estimated rows: 11000.00                                                                                             |
|                                     └── Filter(Probe)                                                                                                            |
|                                         ├── filters: [gt(l1.l_receiptdate (#19), l1.l_commitdate (#18))]                                                         |
|                                         ├── estimated rows: 2400786.33                                                                                           |
|                                         ├── total process time: 59ms                                                                                             |
|                                         └── TableScan                                                                                                            |
|                                             ├── table: default.tpch.lineitem                                                                                     |
|                                             ├── read rows: 7202359                                                                                               |
|                                             ├── read bytes: 42731029                                                                                             |
|                                             ├── partitions total: 9                                                                                              |
|                                             ├── partitions scanned: 9                                                                                            |
|                                             ├── pruning stats: [segments: <range pruning: 9 to 9>, blocks: <range pruning: 9 to 9, bloom pruning: 0 to 0>]       |
|                                             ├── push downs: [filters: [gt(l1.l_receiptdate (#19), l1.l_commitdate (#18))], limit: NONE]                          |
|                                             ├── output columns: [l_orderkey, l_suppkey, l_commitdate, l_receiptdate]                                             |
|                                             └── estimated rows: 7202359.00                                                                                       |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```