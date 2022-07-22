---
title: EXPLAIN FRAGMENTS
---

Shows the logical fragmented execution plan of a SQL statement. 

This command transforms the execution plan of a SQL statement into plan fragments where you can see the process of retrieving the data you need from Databend.

## Syntax

```sql
EXPLAIN FRAGMENTS <statement>
```

## Examples

```sql
EXPLAIN FRAGMENTS select COUNT() from numbers(10) GROUP BY number % 3;

+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                     |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Fragment 0:                                                                                                                                                                 |
|   DataExchange: Shuffle                                                                                                                                                     |
|   AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[COUNT()]]                                                                                                             |
|     Expression: (number % 3):UInt8 (Before GroupBy)                                                                                                                         |
|       ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]] |
|                                                                                                                                                                             |
| Fragment 2:                                                                                                                                                                 |
|   DataExchange: Merge                                                                                                                                                       |
|   Projection: COUNT():UInt64                                                                                                                                                |
|     AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[COUNT()]]                                                                                                             |
|       Remote[receive fragment: 0]                                                                                                                                           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
11 rows in set (0.02 sec)
Read 0 rows, 0.00 B in 0.003 sec., 0 rows/sec., 0.00 B/sec.
```