---
id: architecture
title: Whitepapers
---

## Design Overview

Databend is an open source **elastic** and **scalable** serverless data warehouse, it offers blazing fast query and combines elasticity, simplicity, low cost of the cloud, built to make the Data Cloud easy.

Databend is intended for executing workloads with data stored in cloud storage systems, such as AWS S3 and Azure Blob Storage or others.

We design Databend with the following key functionalities in mind:
1. **Elastic**  In Databend, storage and compute resources can be scaled on demand.
2. **Serverless**  In Databend, you don’t have to think about servers, you pay only for what you actually used.
3. **User-friendly** Databend is an ANSI SQL compliant cloud warehouse, it is easy for data scientist and engineers to use.
4. **Secure** All data files and network traffic in Databend is encrypted end-to-end, and provide Role Based Access Control in SQL level.


![Databend Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

The picture above shows the high-level architecture of Databend, it consists of three components: `meta service` layer, and the  decoupled `compute` and `storage` layers.

## Meta Service Layer

The meta service is a layer to service multiple tenants. This layer implements a persistent key-value store to store each tenant's state.
In current implementation, the meta service has many components:

* Metadata, which manages all metadata of databases, tables, clusters, the transaction, etc.
* Administration, which stores user info, user management, access control information, usage statistics, etc.
* Security, which performs authorization and authentication to protect the privacy of users' data.

The code of `Meta Service Layer` mainly resides in the `store` directory of the repository.

## Compute Layer

The compute layer is the layer to carry out computation for query processing. This layer may consist of many clusters,
and each cluster may consist of many nodes. Each node is a compute unit, and is a collection of components:

* Planner

  The query planner builds an execution plan from the user's SQL statement and represents the query with different types of relational operators (such as `Projection`, `Filter`, `Limit`, etc.).
  
  For example:
  ```
  databend :) EXPLAIN SELECT number + 1 FROM numbers_mt(10) WHERE number > 8 LIMIT 2
  ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ Limit: 2                                                                                                                │
  │   Projection: (number + 1):UInt64                                                                                       │
  │     Expression: (number + 1):UInt64 (Before Projection)                                                                 │
  │       Filter: (number > 8)                                                                                              │
  │         ReadDataSource: scan partitions: [1], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80] │
  └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  ```

* Optimizer

  A rule based optimizer, some rules like predicate push down or pruning of unused columns.

* Processors

  A vector-based query execution pipeline, which is build by planner instructions.
  Each pipeline executor is a processor(such as `SourceTransform`, `FilterTransform`, etc.), it has zero or more inputs and zero or more outputs, and connected as a pipeline, it also can be distributed on multiple nodes judged by your query workload.
  
  For example:
  ```
  databend :) EXPLAIN PIPELINE SELECT number + 1 FROM numbers_mt(10000) WHERE number > 8 LIMIT 2
  ┌─explain───────────────────────────────────────────────────────────────┐
  │ LimitTransform × 1 processor                                          │
  │   Merge (ProjectionTransform × 16 processors) to (LimitTransform × 1) │
  │     ProjectionTransform × 16 processors                               │
  │       ExpressionTransform × 16 processors                             │
  │         FilterTransform × 16 processors                               │
  │           SourceTransform × 16 processors                             │
  └───────────────────────────────────────────────────────────────────────┘
  ```

* Cache

  The cache utilizes local SSDs for caching Data and Indexes based on the version within a node. The cache can be warmed up with different strategies:
  
  * LOAD_ON_DEMAND - Load index or table data on demand(By Default).
  * LOAD_INDEXES - Load indexes only.
  * LOAD_ALL - Load full data and indexes.

Node is the smallest unit of the compute layer, they can be registered as one cluster via namespace.
Many clusters can attach the same database, so they can serve the query in parallel by different users.
When you add new nodes to a cluster, the currently running computational tasks can be scaled(known as work-stealing) guarantee.

The `Compute Layer` codes mainly in the `query` directory.

## Storage Layer

Databend stores data in an efficient, columnar format as Parquet files.
Each Parquet file is sorted by the primary key before being written to the underlying shared storage.
For efficient pruning, Databend also creates indexes for each Parquet file:

* `min_max.idx` The index file stores the *minimum* and *maximum* value of this Parquet file.
* `sparse.idx` The index file store the <key, parquet-page> mapping for every [N] records granularity.

With the indexes, we can speed up the queries by reducing the I/O and CPU cost.
Imagine that Parquet file f1 has `min_max.idx` of `[3, 5)` and Parquet file f2 has `min_max.idx` of `[4, 6)` in column `x`, if the query predicate is `WHERE x < 4`, only f1 needs to be accessed and processed.
