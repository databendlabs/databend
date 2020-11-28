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
| [datasources](src/datasources) | Interface to the datasource([InMemory](src/datasources/memory)/[Csv](src/datasources/csv)/Remote) | WIP |
| [execturos](src/executors) | Executor([EXPLAIN](src/executors/executor_explain.rs)/[SELECT](src/executors/executor_select.rs)) for the Pipeline | WIP |
| [functions](src/functions) | Scalar([Arithmetic](src/functions/function_arithmetic.rs)/[Comparison](src/functions/function_comparison.rs)) and Aggregation([Aggregator](src/functions/function_aggregator.rs)) functions | WIP |
| [processors](src/processors) | Dataflow streaming processor([Pipeline](src/processors/pipeline.rs)) | WIP |
| [planners](src/planners) | Distributed plan for queries and DML statements([SELECT](src/planners/plan_select.rs)/[EXPLAIN](src/planners/plan_explain.rs)) | WIP |
| [servers](src/servers) | Server handler([MySQL](src/servers/mysql)/HTTP) | MySQL |
| [transforms](src/transforms) | Query execution transform([Source](src/transforms/transform_source.rs)/[Filter](src/transforms/transform_filter.rs)) | WIP |

## How to Run?

#### Fuse-Query Cloud Compute starts
```
$make run

12:46:15 [ INFO] Options { log_level: "debug", num_cpus: 8, mysql_handler_port: 3307 }
12:46:15 [ INFO] Fuse-Query Cloud Compute Starts...
12:46:15 [ INFO] Usage: mysql -h127.0.0.1 -P3307
```

#### Query with MySQL client
```
$mysql -h127.0.0.1 -P3307
mysql> explain select a1 from t1 where a > 10 and b < 5 limit 10;

+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| explain                                                                                                                                                                                            |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 └─ Projection: a1
  └─ Limit: 10 (preliminary LIMIT)
    └─ Filter: a > 10 AND b < 5 (WHERE)
      └─ Scan: t1
        └─ ReadDataSource: scan parts [4] (Read from CSV table)                     
 
  └─ Merge (FilterTransform × 4 processors) to (MergeProcessor × 1)
    └─ FilterTransform × 4 processors
      └─ SourceTransform × 4 processors                                                 
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set (0.01 sec)
```

## How to Test?

```
$make test
```

## How to Bench?

```
$make bench
```

