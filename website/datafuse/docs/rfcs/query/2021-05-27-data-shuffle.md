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
'                                               |Plan
'                                               v
'                 +----------+            +-----------+
'                 |          |  Processor |           |
'     Data <------+DataStream|<-----------+Interpreter|
'                 |          |            |           |
'                 +----------+            +-----------+
```

### Parser

### PlanBuilder and Plan

### Optimizer and Plan

### Interpreter and Processor

### DataStream

## Distributed query

### ScatterOptimizer and StagePlan

### PlanScheduler and RemoteProcessor

### Flight API DataStream
