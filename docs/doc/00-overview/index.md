---
title: Welcome
slug: /
---
import GetLatest from '@site/src/components/GetLatest';

Welcome to the Databend documentation! 

Databend is an **open-source**, **elastic**, and **workload-aware** modern cloud data warehouse designed to meet businesses' massive-scale analytics needs at low cost and with low complexity.

This welcome page guides you through the features, architecture, and other important details about Databend.

## Why Databend?

Databend is always searching for and incorporating the most advanced and innovative technologies to provide you with an exceptional user experience.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="whydatabend">
<TabItem value="Performance" label="Performance">

- Blazing-fast data analytics on object storage.
- Leverages data-level parallelism and instruction-level parallelism technologies for optimal performance.
- Supports Git-like MVCC storage for easy querying, cloning, and restoration of historical data.
- No indexes to build, no manual tuning, and no need to figure out partitions or shard data.

</TabItem>

<TabItem value="Compatibility" label="Compatibility">

- Compatible with MySQL / ClickHouse.
- ANSI SQL compliant.
- Easy connection with existing tools such as [MySQL Client](https://databend.rs/doc/integrations/api/mysql-handler), [ClickHouse Client](https://databend.rs/doc/integrations/api/clickhouse-handler), [Vector](https://vector.dev/), [DBeaver](https://dbeaver.com/), [Jupyter](https://databend.rs/doc/integrations/gui-tool/jupyter), [JDBC](https://databend.rs/doc/develop), and more.

</TabItem>

<TabItem value="Data Manipulation" label="Data Manipulation">

- Supports atomic operations such as SELECT, INSERT, DELETE, UPDATE, COPY, and ALTER.
- Provides advanced features such as Time Travel and Multi Catalog (Apache Hive / Apache Iceberg).
- Supports [ingestion of semi-structured data](https://databend.rs/doc/load-data) in various formats like CSV, JSON, and Parquet.
- Supports semi-structured data types such as [ARRAY, MAP, and JSON](https://databend.rs/doc/sql-reference/data-types/data-type-semi-structured-types).

</TabItem>

<TabItem value="Cloud Storage" label="Cloud Storage">

- Supports various cloud storage platforms, including [Amazon S3](https://aws.amazon.com/s3/), [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/), [Google Cloud Storage](https://cloud.google.com/storage/), [Alibaba Cloud OSS](https://www.alibabacloud.com/product/object-storage-service), [Tencent Cloud COS](https://www.tencentcloud.com/products/cos), [Huawei Cloud OBS](https://www.huaweicloud.com/intl/en-us/product/obs.html), [Cloudflare R2](https://www.cloudflare.com/products/r2/), [Wasabi](https://wasabi.com/), and [MinIO](https://min.io).
- Allows instant elasticity, enabling users to scale up or down based on their application needs.

</TabItem>
</Tabs>

## Databend Architucture

This is the high-level architecture of Databend. It consists of three components:
- `meta service layer`
- `compute layer`
- `storage layer`

![Databend Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

### Meta Service Layer

The meta service is a layer to service multiple tenants. This layer implements a persistent key-value store to store each tenant's state.
In the current implementation, the meta service has many components:

- Metadata, which manages all metadata of databases, tables, clusters, the transaction, etc.
- Administration, which stores user info, user management, access control information, usage statistics, etc.
- Security, which performs authorization and authentication to protect the privacy of users' data.

The code of `Meta Service Layer` mainly resides in the `metasrv` directory of the repository.

### Compute Layer

The compute layer is the layer that carries out computation for query processing. This layer may consist of many clusters,
and each cluster may consist of many nodes. Each node is a computing unit and is a collection of components:

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

  A rule-based optimizer, some rules like predicate push down or pruning of unused columns.

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

The `Compute Layer` codes are mainly in the `query` directory.

### Storage Layer

Databend stores data in an efficient, columnar format as Parquet files.
For efficient pruning, Databend also creates indexes for each Parquet file:

- `min_max.idx` The index file stores the *minimum* and *maximum* value of this Parquet file.
- `sparse.idx` The index file store the <key, parquet-page> mapping for every [N] records' granularity.

With the indexes, we can speed up the queries by reducing the I/O and CPU costs.
Imagine that Parquet file f1 has `min_max.idx` of `[3, 5)` and Parquet file f2 has `min_max.idx` of `[4, 6)` in column `x` if the query predicate is `WHERE x < 4`, only f1 needs to be accessed and processed.

## Community

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Get the news fast)
- [Weekly](https://weekly.databend.rs/) (A weekly newsletter about the Databend)

## Roadmap

- [Roadmap v1.0](https://github.com/datafuselabs/databend/issues/9604)
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)
- [Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)

## License

Databend is licensed under Apache 2.0.

## Acknowledgments

- Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture), its computing model is based on [apache-arrow](https://arrow.apache.org/).
- The [documentation website](https://databend.rs) hosted by [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).