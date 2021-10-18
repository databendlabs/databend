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

Databend uses the third-party SQL parser and its AST.
For more information, see: https://github.com/ballista-compute/sqlparser-rs

### PlanBuilder and Plan

A query plan (or query execution plan) is a sequence of steps used to access data in Databend. It is built by PlanBuilder from AST. We also use tree to describe it(similar to AST). But it has some differences with AST:

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
  Projection: (number % 3) as key:UInt8, SUM(number) as value:UInt64
    Sort: (number % 3):UInt8,
      AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]
        AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]
          Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)
            Filter: ((number > 10) AND (number < 990))
              ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000, read_bytes: 8000] |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Optimizer and Plan

For a query, especially a complex query, you can used different plan combinations, orders and structures to get the data . Each of the different ways will get different processing time. So we need to find a reasonable plan combination way in the shortest time, which is what the optimizer does.

### Interpreter and Processor

The interpreter constructs the optimized plan into an executable data stream. We pull the result of SQL by pulling the data in the stream. The calculation logic of each operator in SQL corresponds to a processor, such as FilterPlan -> FilterProcessor, ProjectionPlan -> ProjectionProcessor


## Distributed query

In the cluster mode, we may have to process with some problems different from the standalone mode.

- In distributed mode, the tables to be queried are always distributed in different nodes
- For some scenarios, distributed processing is always efficient, such as GROUP BY with keys, JOIN
- For some scenarios, we have no way of distributed processing, such as LIMIT, GROUP BY without keys
- In order to ensure fast calculation, we need to coordinate the location of calculation and data.

Let's see how normal queries run on a database cluster.

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

In Databend, we use ScatterOptimizer to decide the distributed computing of query. In other words, distributed query is an optimization of standalone query.

In ScatterOptimizer, we traverse all the plans of the query and rewrite the plan of interest(rewrite as StagePlan { kind:StageKind, input:Self }), where input is the rewritten plan, and kind is an enumeration(Normal: data is shuffled again, Expansive: data spreads from one node to multiple nodes, Convergent: data aggregation from multiple nodes to one node)

### PlanScheduler and RemoteProcessor

In cluster mode, we extract all the StagePlans in the plan optimized by ScatterOptimizer and send them to the corresponding nodes in the cluster according to the kind.

For example:
```text
mysql> EXPLAIN SELECT argMin(user, salary)  FROM (SELECT sum(number) AS salary, number%3 AS user FROM numbers_local(1000000000) GROUP BY user);
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Projection: argMin(user, salary):UInt64   <-- execute in local node
  AggregatorFinal: groupBy=[[]], aggr=[[argMin(user, salary)]]
    RedistributeStage[expr: 0]    <-- execute in all nodes of the cluster
      AggregatorPartial: groupBy=[[]], aggr=[[argMin(user, salary)]]
        Projection: sum(number) as salary:UInt64, (number % 3) as user:UInt8
  AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[sum(number)]]
    RedistributeStage[expr: sipHash(_group_by_key)]   <-- execute in all nodes of the cluster
      AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[sum(number)]]
        Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)
          RedistributeStage[expr: blockNumber()]    <-- execute in local node
            ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 1000000000, read_bytes: 8000000000] |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Flight API DataStream
We need to fetch the results of the plans sent to other nodes for execution in some way. FuseData uses the third-party library arrow-flight. more information:[https://github.com/apache/arrow-rs/tree/master/arrow-flight]
