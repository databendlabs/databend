---
id: explain-statement
title: EXPLAIN
---

Show the execution plan of a statement.

Syntax:
```text
EXPLAIN SELECT ... 
```


## Explain

```text
mysql> EXPLAIN SELECT sum(number+3)/count(number) FROM system.numbers(100000);

+--------------------------------------------------------------------------------+
| explain                                                                        |
+--------------------------------------------------------------------------------+
| └─ Aggregate: (sum([(number + 3)]) / count([number])):UInt64
  └─ ReadDataSource: scan parts [4](Read from system.numbers_mt table, Read Rows:10000, Read Bytes:80000)        
| 
  └─ AggregateFinalTransform × 1 processor
    └─ Merge (AggregatePartialTransform × 4 processors) to (MergeProcessor × 1)
     └─ AggregatePartialTransform × 4 processors
       └─ SourceTransform × 4 processors                      
+----------------------------------------------------------------------------------+
2 rows in set (0.01 sec)
```
