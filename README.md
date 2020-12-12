[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Lint/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Lint%22)
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Test/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Test%22)
[![codecov.io](https://codecov.io/gh/datafusedev/fuse-query/graphs/badge.svg)](https://codecov.io/gh/datafusedev/fuse-query/branch/master)
[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://opensource.org/licenses/AGPL-3.0)

# FuseQuery

FuseQuery is a Distributed SQL Query Engine at scale.

New implementation of ClickHouse from scratch in Rust, WIP.

Give thanks to [ClickHouse](https://github.com/ClickHouse/ClickHouse) and [Arrow](https://github.com/apache/arrow).

## Features

* **High Performance**
* **High Scalability**
* **High Reliability**


## Architecture

| Crate     | Description |  Status |
|-----------|-------------|-------------|
| optimizers | Optimizer for distributed plan | TODO |
| distributed | Distributed scheduler and executor for planner | TODO |
| [datablocks](src/datablocks) | Vectorized data processing unit | WIP |
| [datastreams](src/datastreams) | Async streaming iterators | WIP |
| [datasources](src/datasources) | Interface to the datasource([system.numbers for performance](src/datasources/system)/Remote(S3 or other table storage engine)) | WIP |
| [execturos](src/executors) | Executor([EXPLAIN](src/executors/executor_explain.rs)/[SELECT](src/executors/executor_select.rs)) for the Pipeline | WIP |
| [functions](src/functions) | Scalar([Arithmetic](src/functions/function_arithmetic.rs)/[Comparison](src/functions/function_comparison.rs)) and Aggregation([Aggregator](src/functions/function_aggregator.rs)) functions | WIP |
| [processors](src/processors) | Dataflow streaming processor([Pipeline](src/processors/pipeline.rs)) | WIP |
| [planners](src/planners) | Distributed plan for queries and DML statements([SELECT](src/planners/plan_select.rs)/[EXPLAIN](src/planners/plan_explain.rs)) | WIP |
| [servers](src/servers) | Server handler([MySQL](src/servers/mysql)/HTTP) | MySQL |
| [transforms](src/transforms) | Query execution transform([Source](src/transforms/transform_source.rs)/[Filter](src/transforms/transform_filter.rs)/[Projection](src/transforms/transform_projection.rs)/[AggregatorPartial](src/transforms/transform_aggregate_partial.rs)/[AggregatorFinal](src/transforms/transform_aggregate_final.rs)/[Limit](src/transforms/transform_limit.rs)) | WIP |

## Performance

* Dataset: 10,000,000,000 (10 Billion), system.numbers_mt 
* Hardware: 8vCPUx16G KVM Cloud Instance


|Query |FuseQuery Cost| ClickHouse Cost|
|-------------------------------|---------------| ----|
|SELECT sum(number) FROM system.numbers_mt(10000000000) | [2.04s] | [1.34s], 7.48 billion rows/s., 59.80 GB/s|
|SELECT max(number) FROM system.numbers_mt(10000000000) | [3.66s] | [2.33s], 4.34 billion rows/s., 34.74 GB/s|
|SELECT count(number) FROM system.numbers_mt(10000000000) | [1.63s] | [0.67s], 15.00 billion rows/s., 119.99 GB/s|
|SELECT sum(number) / count(number) FROM system.numbers_mt(10000000000) | [2.04s] | [1.28s], 7.84 billion rows/s., 62.73 GB/s|
|SELECT sum(number) / count(number), max(number), min(number) FROM system.numbers_mt(10000000000) | [7.97s] | [4.30s], 2.33 billion rows/s., 18.61 GB/s|

Note:
* ClickHouse system.numbers_mt is <b>8-way</b> parallelism processing
* FuseQuery system.numbers_mt is <b>8-way</b> parallelism processing

```
fuse-query> explain select count(number) from system.numbers_mt(10000000000);
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                      |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| └─ Aggregate: count([number])
  └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table)                                                                                                                                            |
| 
  └─ AggregateFinalTransform × 1 processor
    └─ Merge (AggregatePartialTransform × 8 processors) to (MergeProcessor × 1)
      └─ AggregatePartialTransform × 8 processors
        └─ SourceTransform × 8 processors                      |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

clickhouse> explain pipeline select count(number) from system.numbers_mt(10000000000);
┌─explain───────────────────────────┐
│ (Expression)                      │
│ ExpressionTransform               │
│   (Expression)                    │
│   ExpressionTransform             │
│     (Aggregating)                 │
│     Resize 8 → 1                  │
│       AggregatingTransform × 8    │
│         (Expression)              │
│         ExpressionTransform × 8   │
│           (SettingQuotaAndLimits) │
│             (ReadFromStorage)     │
│             NumbersMt × 8 0 → 1   │
└───────────────────────────────────┘
```

## How to install Rust(nightly)?
```
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
$ rustup toolchain install nightly
```


## How to Run?

#### Fuse-Query Server
```
$ make run

12:46:15 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
12:46:15 [ INFO] Fuse-Query Cloud Compute Starts...
12:46:15 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

#### Query with MySQL client
###### Connect
```
$ mysql -h127.0.0.1 -P3307
```

###### Explain
```
mysql> explain select number as a, number/2 as b, number+1 as c  from system.numbers(10000000) where number < 4 limit 10;
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                                                                                               |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| └─ Limit: 10
  └─ Projection: number as a, number / 2 as b, number + 1 as c
    └─ Filter: number < 4
      └─ ReadDataSource: scan parts [8](Read from system.numbers table)                                                                                                                                         |
| 
  └─ LimitTransform × 1 processor
    └─ Merge (LimitTransform × 8 processors) to (MergeProcessor × 1)
      └─ LimitTransform × 8 processors
        └─ ProjectionTransform × 8 processors
          └─ FilterTransform × 8 processors
            └─ SourceTransform × 8 processors                                |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set (0.01 sec)

```

###### Select
```
mysql> select number as a, number/2 as b, number+1 as c  from system.numbers(10000000) where number < 4 limit 10;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    0 |    0 |    1 |
|    1 |    0 |    2 |
|    2 |    1 |    3 |
|    3 |    1 |    4 |
+------+------+------+
4 rows in set (0.10 sec)
```

## How to Test?

```
$ make test
```
