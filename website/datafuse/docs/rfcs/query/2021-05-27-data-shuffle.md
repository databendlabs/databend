# Distributed query and data shuffle

## Summary

Distributed query is distributed database necessary feature.
This doc is intended to explain the distributed query and its data flow design.

## Local query

Let's see how normal queries run on a single database node.

``` text
'        +------+       +------------+      +---------+
'        |      |  AST  |            | Plan |         |
' SQL--->|Parser+------>|Plan Builder+----->|Optimizer|
'        |      |       |            |      |         |
'        +------+       +------------+      +---+-----+
'                                               | Plan
'                                               v
'                 +----------+            +-----------+
'                 |          |  Processor |           |
'     Data <------+DataStream|<-----------+Interpreter|
'                 |          |            |           |
'                 +----------+            +-----------+
```

### Parser and AST

DataFuse uses the third-party SQL parser and its AST.
For more information, see: https://github.com/ballista-compute/sqlparser-rs

### PlanBuilder and Plan

A query plan (or query execution plan) is a sequence of steps used to access data in DataFuse. It is built by PlanBuilder from AST. We also use tree to describe it(similar to AST). But it has some differences with AST:

- Plan is serializable and deserializable.
- Plan is grammatically safe, we don't worry about it.
- Plan is used to describe the computation and data dependency, not related to syntax priority
- We can show it with `EXPLAIN SELECT ...`

``` text
mysql> EXPLAIN SELECT number % 3 AS key, SUM(number) AS value FROM numbers(1000) WHERE number > 10 AND number < 990 GROUP BY key ORDER BY key ASC LIMIT 10;
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Limit: 10
  Projection: (number % 3) as key:UInt64, SUM(number) as value:UInt64
    Sort: (number % 3):UInt64
      AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]
        AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]
          Expression: (number % 3):UInt64, number:UInt64 (Before GroupBy)
            Filter: ((number > 10) AND (number < 990))
              ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000] |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Optimizer and Plan

For a query, especially a complex query, you can used different plan combinations, orders and structures to get the data . Each of the different ways will get different processing time. So we need to find a reasonable plan combination way in the shortest time, which is what the optimizer does.

### Interpreter and Processor

### DataStream

## Distributed query

```text
'                                              +------+       +------------+      +------------------+
'                                              |      |  AST  |            | Plan |    Optimizer     |
'                                       SQL--->|Parser+------>|Plan Builder+----->|                  |
'                                              |      |       |            |      | ScatterOptimizer |
'                                              +------+       +------------+      +--------+---------+
'                                                                                          |
'                                        +--------------+                                  |
'                                        |              |                                  |
'                                     +--+ FlightStream | <------+                         |  Plan
'                                     |  |              |        |                         |
'                                     |  +--------------+        |                         |
'                                     |                          |                         |
'                                     |                          |                         |
'                                     |                          |    Flight RPC           v
'        +----------+    Processor    |  +--------------+        |                  +----------------+
'        |          | RemoteProcessor |  |              |        |     do_action    |  Interpreter   |
' Data<--+DataStream|<----------------+--+ FlightStream | <------+------------------+                |
'        |          |                 |  |              |        |                  | PlanRescheduler|
'        +----------+                 |  +--------------+        |                  +----------------+
'                                     |                          |
'                                     |                          |
'                                     |                          |
'                                     |                          |
'                                     |  +--------------+        |
'                                     |  |              |        |
'                                     +--+ FlightStream | <------+
'                                        |              |
'                                        +--------------+
```

### ScatterOptimizer and StagePlan

### PlanScheduler and RemoteProcessor

### Flight API DataStream
