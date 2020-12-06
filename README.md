[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Lint/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Lint%22)
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Test/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Test%22)
[![codecov.io](https://codecov.io/gh/datafusedev/fuse-query/graphs/badge.svg)](https://codecov.io/gh/datafusedev/fuse-query/branch/master)
[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://opensource.org/licenses/AGPL-3.0)

# FuseQuery

FuseQuery is a Distributed SQL Query Engine at scale.

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
| [transforms](src/transforms) | Query execution transform([Source](src/transforms/transform_source.rs)/[Filter](src/transforms/transform_filter.rs)/[Projection](src/transforms/transform_projection.rs)/[Aggregator](src/transforms/transform_aggregate.rs)/[Limit](src/transforms/transform_limit.rs)) | WIP |

## How to Run?

#### Fuse-Query Cloud Compute starts
```
$make run

12:46:15 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
12:46:15 [ INFO] Fuse-Query Cloud Compute Starts...
12:46:15 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

#### Query with MySQL client
###### Connect
```
$mysql -h127.0.0.1 -P3307
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
      └─ ReadDataSource: scan parts [4](Read from system.numbers table)                                                                                                                                         |
| 
  └─ LimitTransform × 1 processor
    └─ Merge (LimitTransform × 4 processors) to (MergeProcessor × 1)
      └─ LimitTransform × 4 processors
        └─ ProjectionTransform × 4 processors
          └─ FilterTransform × 4 processors
            └─ SourceTransform × 4 processors                                |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set (0.41 sec)

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
4 rows in set (0.81 sec)
```

###### Server log in 4-parallels
```
12:28:09 [DEBUG] (33) fuse_query::datasources::system::numbers_stream: number stream part:10000000-2500000-4999999, cost:129.60938ms
12:28:09 [DEBUG] (34) fuse_query::datasources::system::numbers_stream: number stream part:10000000-0-2499999, cost:130.609545ms
12:28:09 [DEBUG] (30) fuse_query::datasources::system::numbers_stream: number stream part:10000000-7500000-9999999, cost:154.209505ms
12:28:09 [DEBUG] (28) fuse_query::datasources::system::numbers_stream: number stream part:10000000-5000000-7499999, cost:155.297662ms
12:28:09 [DEBUG] (34) fuse_query::transforms::transform_projection: transform projection cost:177.497µs
12:28:09 [DEBUG] (33) fuse_query::transforms::transform_projection: transform projection cost:58.612µs
12:28:09 [DEBUG] (28) fuse_query::transforms::transform_projection: transform projection cost:72.562µs
12:28:09 [DEBUG] (30) fuse_query::transforms::transform_projection: transform projection cost:72.598µs
12:28:09 [DEBUG] (10) fuse_query::servers::mysql::mysql_handler: MySQLHandler executor cost:408.81587ms
12:28:09 [DEBUG] (10) fuse_query::servers::mysql::mysql_handler: MySQLHandler send to client cost:211.087µs
```

## How to Test?

```
$make test
```

## How to Bench?

```
$make bench

select number from system.numbers(1000000) where number < 4 limit 10                                                                             
                        time:   [4.5042 ms 4.5400 ms 4.5764 ms]
                        change: [-0.8063% +0.0880% +1.2691%] (p = 0.87 > 0.05)
                        No change in performance detected.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

select number as a, number/2 as b, number+1 as c from system.numbers(1000000) where number < 4 limit...                                                                             
                        time:   [4.5120 ms 4.5444 ms 4.5760 ms]
                        change: [-4.1011% -3.0361% -2.0114%] (p = 0.00 < 0.05)
                        Performance has improved.
```

