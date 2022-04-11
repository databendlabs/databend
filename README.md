<div align="center">

<p align="center"><img alt="Databend Logo" src="website/static/img/favicon.svg" width="20%"/></p>
<p align="center">A Modern Cloud Data Warehouse with the Elasticity and Performance both on Object Storage</p>
 
<h4 align="center">
  <a href="https://databend.rs/doc">Documentation</a>  |
  <a href="https://perf.databend.rs">Benchmarking</a>  |
  <a href="https://github.com/datafuselabs/databend/issues/4591">Roadmap(v0.8)</a>

</h4>

<div>
<a href="https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA">
<img src="https://badgen.net/badge/Slack/Join%20Databend/0abd59?icon=slack" alt="slack" />
</a>

<a href="https://github.com/datafuselabs/databend/actions">
<img src="https://github.com/datafuselabs/databend/actions/workflows/databend-release.yml/badge.svg" alt="CI Status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="patform" />

<a href="https://opensource.org/licenses/Apache-2.0">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="license" />
</a>

</div>
</div>
<br>

- [What is Databend?](#what-is-databend)
- [Design Overview](#design-overview)
   - [Meta Service Layer](#meta-service-layer)
   - [Compute Layer](#compute-layer)
   - [Storage Layer](#storage-layer)
- [Getting Started](#getting-started)
- [Use Case](#use-case)
- [Community](#community)
- [Roadmap](#roadmap)

## What is Databend?

Databend is an open source **Elastic** and **Workload-Aware** Modern Cloud Data Warehouse.

Databend uses the latest techniques in vectorized query processing to allow you to do blazing-fast data analytics on Object Storage.

- __Instant Elasticity__

  Databend separates the storage and compute, which allows you easily scale up or scale down based on your application's needs.

- __Blazing Performance__

  Databend leverages data-level parallelism(Vectorized Query Execution) and instruction-level parallelism(SIMD) technology, offers blazing performance data analytics.

- __Support for Semi-Structured Data__

  Databend supports [ingestion of semi-structured data](https://databend.rs/doc/load-data) in various formats like CSV, JSON and Parquet which located in cloud or your local file system.

- __MySQL/ClickHouse Compatible__

  Databend is ANSI SQL compliant and MySQL/ClickHouse wire protocol compatible, making it easy to connect with existing tools.

- __Easy to Use__

  Databend has no indexes to build, no manual tuning required, no manual figuring out partitions or shard data, it’s all done for you as data is loaded into table. 
 
## Design Overview

This is the high-level architecture of Databend, it consists of three components:
- `meta service layer`
- `compute layer`
- `storage layer`

![Databend Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

### Meta Service Layer

The meta service is a layer to service multiple tenants. This layer implements a persistent key-value store to store each tenant's state.
In current implementation, the meta service has many components:

- Metadata, which manages all metadata of databases, tables, clusters, the transaction, etc.
- Administration, which stores user info, user management, access control information, usage statistics, etc.
- Security, which performs authorization and authentication to protect the privacy of users' data.

The code of `Meta Service Layer` mainly resides in the `metasrv` directory of the repository.

### Compute Layer

The compute layer is the layer to carry out computation for query processing. This layer may consist of many clusters,
and each cluster may consist of many nodes. Each node is a compute unit, and is a collection of components:

- **Planner**

  The query planner builds an execution plan from the user's SQL statement and represents the query with different types of relational operators (such as `Projection`, `Filter`, `Limit`, etc.).

  For example:
  ```
  databend :) EXPLAIN SELECT avg(number) FROM numbers(100000) GROUP BY number % 3
  ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ Projection: avg(number):Float64                                                                                                                                                         │
  │   AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[avg(number)]]                                                                                                                       │
  │     AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[avg(number)]]                                                                                                                   │
  │       Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)                                                                                                                    │
  │         ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000, read_bytes: 800000, partitions_scanned: 11, partitions_total: 11], push_downs: [projections: [0]] │
  └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  ```

- **Optimizer**

  A rule based optimizer, some rules like predicate push down or pruning of unused columns.

- **Processors**

  A Pull&Push-Based query execution pipeline, which is built by planner instructions.
  Each pipeline executor is a processor(such as `SourceTransform`, `FilterTransform`, etc.), it has zero or more inputs and zero or more outputs, and connected as a pipeline, it also can be distributed on multiple nodes judged by your query workload.

  For example:
  ```
  databend :) EXPLAIN PIPELINE SELECT avg(number) FROM numbers(100000) GROUP BY number % 3
  ┌─explain────────────────────────────────────────────────────────────────────────────────┐
  │ ProjectionTransform × 16 processors                                                    │
  │   Mixed (GroupByFinalTransform × 1 processor) to (ProjectionTransform × 16 processors) │
  │     GroupByFinalTransform × 1 processor                                                │
  │       Merge (GroupByPartialTransform × 16 processors) to (GroupByFinalTransform × 1)   │
  │         GroupByPartialTransform × 16 processors                                        │
  │           ExpressionTransform × 16 processors                                          │
  │             SourceTransform × 16 processors                                            │
  └────────────────────────────────────────────────────────────────────────────────────────┘
  ```

Node is the smallest unit of the compute layer. A set of nodes can be registered as one cluster via namespace.
Many clusters can attach the same database, so they can serve the query in parallel by different users.
When you add new nodes to a cluster, the currently running computational tasks can be scaled(known as work-stealing) guarantee.

The `Compute Layer` codes mainly in the `query` directory.

### Storage Layer

Databend stores data in an efficient, columnar format as Parquet files.
Each Parquet file is sorted by the primary key before being written to the underlying shared storage.
For efficient pruning, Databend also creates indexes for each Parquet file:

- `min_max.idx` The index file stores the *minimum* and *maximum* value of this Parquet file.
-`sparse.idx` The index file store the <key, parquet-page> mapping for every [N] records granularity.

With the indexes, we can speed up the queries by reducing the I/O and CPU cost.
Imagine that Parquet file f1 has `min_max.idx` of `[3, 5)` and Parquet file f2 has `min_max.idx` of `[4, 6)` in column `x`, if the query predicate is `WHERE x < 4`, only f1 needs to be accessed and processed.

## Getting Started

- [Databend Deploy](https://databend.rs/doc/deploy)
- [Databend Develop](https://databend.rs/doc/develop)
- [Databend Contributing](https://databend.rs/doc/contributing)
- [Databend Performance](https://databend.rs/doc/performance)
- [Databend Continuous Benchmarking](https://perf.databend.rs)

## Use Case

- [Analyzing Github Repository with Databend](https://databend.rs/doc/learn/analyze-github-repo-with-databend)
- [Analyzing Nginx Access Logs with Databend](https://databend.rs/doc/learn/analyze-nginx-logs-with-databend-and-vector)

## Community

For general help to use Databend, please refer to the official documentation. For additional help, you can use one of these channels to ask a question:

- [Slack](https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA) (For live discussion with the Community)
- [Github](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Get the news fast)
- [Weekly](https://weekly.databend.rs/) (A weekly newsletter about Databend)

## Roadmap
- [Roadmap v0.8](https://github.com/datafuselabs/databend/issues/4591)
- [Roadmap 2022](https://github.com/datafuselabs/databend/issues/3706)

## License

Databend is licensed under [Apache 2.0](LICENSE).

## Acknowledgement

- Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture), its computing model is based on [apache-arrow](https://arrow.apache.org/).
- The [documentation website](https://databend.rs) hosted by [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
