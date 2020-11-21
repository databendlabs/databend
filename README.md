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
| processors | Dataflow streaming processor | WIP |
| transforms | Query execution transform | WIP |
| planners | Distributed plan for queries and DML statements | WIP |
| optimizers | Optimizer for distributed plan | TODO |
| functions | Scalar and Aggregation functions | WIP |
| datablocks | Vectorized data processing unit | WIP |
| datastreams | Async streaming iterators | WIP |
| datasources | Interface to the fuse-store server | WIP | 
| distributed | Distributed scheduler and executor for planner | TODO |
| servers | Server handler(MySQL/HTTP) | MySQL |

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
| └─ Projection: a1
  └─ Limit: 10 (preliminary LIMIT)
    └─ Filter: a > 10 AND b < 5 (WHERE)
      └─ Scan: t1
        └─ ReadDataSource: scan parts [4] (Read from CSV table)                     |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## How to Test?

```
$make test
```

## How to Bench?

```
$make bench
```

