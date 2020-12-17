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

## Status
#### SQL Support

- [x] Projection
- [x] Filter (WHERE)
- [x] Limit
- [x] Aggregate
- [x] Common math functions
- [ ] Sorting
- [ ] Subqueries
- [ ] Joins


## Architecture

| Crate     | Description |  Status |
|-----------|-------------|-------------|
| distributed | Distributed scheduler and executor for planner | TODO |
| [optimizers](src/optimizers) | Optimizer for distributed plan | WIP |
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
* Rust: rustc 1.50.0-nightly (f76ecd066 2020-12-15)

|Query |FuseQuery Cost| ClickHouse Cost|
|-------------------------------|---------------| ----|
|SELECT sum(number)  | [1.77s] | [1.34s], 7.48 billion rows/s., 59.80 GB/s|
|SELECT max(number)| [2.83s] | [2.33s], 4.34 billion rows/s., 34.74 GB/s|
|SELECT max(number+1)| [6.13s] | [3.29s], 3.04 billion rows/s., 24.31 GB/s|
|SELECT count(number) | [1.55s] | [0.67s], 15.00 billion rows/s., 119.99 GB/s|
|SELECT sum(number) / count(number) | [2.04s] | [1.28s], 7.84 billion rows/s., 62.73 GB/s|
|SELECT sum(number) / count(number), max(number), min(number)| [6.40s] | [4.30s], 2.33 billion rows/s., 18.61 GB/s|

Note:
* ClickHouse system.numbers_mt is <b>8-way</b> parallelism processing
* FuseQuery system.numbers_mt is <b>8-way</b> parallelism processing

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

or run with docker:

```
$ docker pull datafusedev/fuse-query
...

$ docker run --rm -p 3307:3307 docker.io/datafusedev/fuse-query
05:12:36 [ INFO] Options { log_level: "debug", num_cpus: 6, mysql_handler_port: 3307 }
05:12:36 [ INFO] Fuse-Query Cloud Compute Starts...
05:12:36 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

#### Query with MySQL client

###### Connect

```
$ mysql -h127.0.0.1 -P3307
```

###### Explain

```
mysql> explain select (number+1) as c1, number/2 as c2 from system.numbers_mt(10000000) where (c1+c2+1) < 100 limit 3;
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                                                                                                                                               |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| └─ Limit: 3
  └─ Projection: (number + 1) as c1, (number / 2) as c2
    └─ Filter: ((((number + 1) + (number / 2)) + 1) < 100)
      └─ ReadDataSource: scan parts [8](Read from system.numbers_mt table)                                                                                                             |
| 
  └─ LimitTransform × 1 processor
    └─ Merge (LimitTransform × 8 processors) to (MergeProcessor × 1)
      └─ LimitTransform × 8 processors
        └─ ProjectionTransform × 8 processors
          └─ FilterTransform × 8 processors
            └─ SourceTransform × 8 processors                                |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set (0.00 sec)
```

###### Select

```
mysql> select (number+1) as c1, number/2 as c2 from system.numbers_mt(10000000) where (c1+c2+1) < 100 limit 3;
+------+------+
| c1   | c2   |
+------+------+
|    1 |    0 |
|    2 |    0 |
|    3 |    1 |
+------+------+
3 rows in set (0.06 sec)
```

## How to Test?

```
$ make test
```
