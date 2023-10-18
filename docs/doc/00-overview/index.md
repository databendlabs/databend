---
title: Welcome
slug: /
---

Welcome to the Databend documentation! Databend is an open-source, Elastic, Workload-Aware cloud data warehouse engineered for blazing-speed data analytics at a massive scale. **Crafted with Rust, it's your most efficient [alternative to Snowflake](https://github.com/datafuselabs/databend/issues/13059)**.

This welcome page guides you through the features, architecture, and other important details about Databend.

## Why Databend?

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="whydatabend">
<TabItem value="Performance" label="Performance">

- Blazing-fast data analytics on object storage.
- Leverages data-level parallelism and instruction-level parallelism technologies for [optimal performance](https://benchmark.clickhouse.com/).
- Supports Git-like MVCC storage for easy querying, cloning, and restoration of historical data.
- No indexes to build, no manual tuning, and no need to figure out partitions or shard data.

</TabItem>

<TabItem value="Data Manipulation" label="Data Manipulation">

- Supports atomic operations such as `SELECT`, `INSERT`, `DELETE`, `UPDATE`, `REPLACE`, `COPY`, and `MERGE`.
- Provides advanced features such as Time Travel and Multi Catalog (Apache Hive / Apache Iceberg).
- Supports [ingestion of semi-structured data](https://databend.rs/doc/load-data/load) in various formats like CSV, JSON, and Parquet.
- Supports semi-structured data types such as [ARRAY, MAP, and JSON](https://databend.rs/doc/sql-reference/data-types/).

</TabItem>

<TabItem value="Object Storage" label="Object Storage">

- Supports various object storage platforms. Click [here](../10-deploy/00-understanding-deployment-modes.md#supported-object-storage) to see a full list of supported platforms.
- Allows instant elasticity, enabling users to scale up or down based on their application needs.

</TabItem>
</Tabs>

## Databend Architecture

Databend's high-level architecture is composed of a meta-service layer, a compute layer, and a storage layer.

![Databend Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

<Tabs groupId="databendlay">
<TabItem value="Meta-Service Layer" label="Meta-Service Layer">

Databend has the capability to support multiple tenants, and the meta-service layer serves these tenants and stores their respective states in a persistent key-value store. The meta-service layer plays a vital management role in Databend by:

- Managing all metadata related to databases, tables, clusters, transactions, and more.
- Storing user information, access control data, usage statistics, and other related information.
- Performing user authentication and authorization to ensure a secure environment.

If you're interested, you can find the code related to the meta-service layer in the [meta](https://github.com/datafuselabs/databend/tree/main/src/meta) folder of the GitHub repository.

</TabItem>
<TabItem value="Compute Layer" label="Compute Layer">

The compute layer's main responsibility is to execute computations for queries, and it can include several clusters, each with multiple nodes.

To form a cluster, several nodes can be grouped together by assigning them a shared namespace. This allows clusters to access the same database, which facilitates the handling of multiple user queries concurrently. By adding new nodes to a cluster, it is possible to scale up the computational tasks that are already running. This process, known as "work-stealing", ensures that the system can continue to operate efficiently even with additional nodes.

A node serves as a fundamental unit of the computer layer and is composed of the following components:

- Planner: The planner creates a plan for executing the SQL statement provided by the user. This plan includes various types of operators from [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra), such as Projection, Filter, and Limit, to represent the query.

```sql
EXPLAIN SELECT avg(number) FROM numbers(100000) GROUP BY number % 3

explain                                                   |
----------------------------------------------------------+
EvalScalar                                                |
├── expressions: [avg(number) (#3)]                       |
└── AggregateFinal                                        |
    ├── group by: [(number % 3)]                          |
    ├── aggregate functions: [avg(number)]                |
    └── AggregatePartial                                  |
        ├── group by: [(number % 3)]                      |
        ├── aggregate functions: [avg(number)]            |
        └── EvalScalar                                    |
            ├── expressions: [%(numbers.number (#0), 3)]  |
            └── TableScan                                 |
                ├── table: default.system.numbers         |
                ├── read rows: 100000                     |
                ├── read bytes: 800000                    |
                ├── partitions total: 2                   |
                ├── partitions scanned: 2                 |
                └── push downs: [filters: [], limit: NONE]|
```

- Optimizer: The optimizer is a rule-based optimizer that employs a set of predefined rules, such as "predicate pushdown" and "pruning of unused columns", to determine the most efficient way to execute a query.

- Processors: A query execution pipeline is a series of steps that retrieves data for a given query. Databend builds the query execution pipeline using planner instructions and follows a Pull&Push approach, meaning it can both pull and push data as needed. Each step in the pipeline is called a "processor", such as SourceTransform or FilterTransform, and can have multiple inputs and outputs. The processors are connected to form a pipeline that executes the query. To optimize performance, the pipeline can be distributed across multiple nodes depending on the query workload.

```sql
EXPLAIN PIPELINE SELECT avg(number) FROM numbers(100000) GROUP BY number % 3

explain                                                                              |
-------------------------------------------------------------------------------------+
CompoundChunkOperator(Rename) × 1 processor                                          |
  CompoundChunkOperator(Project) × 1 processor                                       |
    CompoundChunkOperator(Map) × 1 processor                                         |
      GroupByFinalTransform × 1 processor                                            |
        Merge (GroupByPartialTransform × 2 processors) to (GroupByFinalTransform × 1)|
          GroupByPartialTransform × 2 processors                                     |
            CompoundChunkOperator(Map) × 2 processors                                |
              CompoundChunkOperator(Project->Rename) × 2 processors                  |
                NumbersSourceTransform × 2 processors                                |
```

If you're interested, you can find the code related to the compute layer in the [query](https://github.com/datafuselabs/databend/tree/main/src/query) folder of the GitHub repository.

</TabItem>
<TabItem value="Storage Layer" label="Storage Layer">

Databend uses an open-source and efficient columnar format known as [Parquet](https://parquet.apache.org/) to store data. To make querying faster, Databend also creates indexes for each Parquet file. There are two types of indexes:

- min_max.idx: Stores the minimum and maximum value of the Parquet file.
- sparse.idx: Stores the mapping of the key and the corresponding Parquet page for every [N] records' granularity.

With these indexes, Databend can speed up queries by reducing the I/O and CPU costs. For example, if Parquet file f1 has min_max.idx of [3, 5) and Parquet file f2 has min_max.idx of [4, 6) in column x, and the query is looking for values where x is less than 4, only f1 needs to be accessed and processed.

</TabItem>
</Tabs>

## Community

The Databend community is open to data professionals, students, and anyone who has a passion for cloud data warehouses. Feel free to click on the links below to be a part of the excitement:

- Slack: https://link.databend.rs/join-slack
- GitHub: https://github.com/datafuselabs/databend
- Twitter: https://twitter.com/DatabendLabs
- LinkedIn: https://www.linkedin.com/company/datafuselabs
- YouTube: https://www.youtube.com/@DatabendLabs

## Roadmap

- [Roadmap v1.3](https://github.com/datafuselabs/databend/issues/11868)
- [Roadmap v1.2](https://github.com/datafuselabs/databend/issues/11073)
- [Roadmap v1.1](https://github.com/datafuselabs/databend/issues/10334)
- [Roadmap v1.0](https://github.com/datafuselabs/databend/issues/9604)
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)
- [Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)

## License

Databend is licensed under Apache 2.0.

## Acknowledgments

- Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture), its computing model is based on [apache-arrow](https://arrow.apache.org/).
- The [documentation website](https://databend.rs) hosted by [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
