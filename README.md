<div align="center">
<h1>Databend</h1>
<strong>The Open Source Cloud Warehouse for Everyone</strong>
<br>
 https://databend.rs
<br>
<br>

<div>
<a href="https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA">
<img src="https://badgen.net/badge/Slack/Join%20Datafuse/0abd59?icon=slack" alt="slack" />
</a>

<a href="https://github.com/datafuselabs/databend/actions">
<img src="https://github.com/datafuselabs/databend/actions/workflows/unit-tests.yml/badge.svg" alt="CI Status" />
</a>

<a href="https://codecov.io/gh/datafuselabs/databend">
<img src="https://codecov.io/gh/datafuselabs/databend/branch/master/graph/badge.svg" alt="codecov" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux,%20ARM,%20OS%20X,%20Windows-green.svg?style=flat" alt="patform" />

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
- [Roadmap](#roadmap)

## What is Databend?

Databend aimed to be an open source **elastic** and **reliable** cloud warehouse, it offers blazing fast query and combines elasticity, simplicity, low cost of the cloud, built to make the Data Cloud easy.

Databend design principles:
1. **Elastic**  In Databend, storage and compute resources can dynamically scale up and down on demand.
2. **Secure** All data files and network traffic in Databend is encrypted end-to-end, and provide Role Based Access Control in SQL level.
3. **User-friendly** Databend is an ANSI SQL compliant cloud warehouse, it is easy for data scientist and engineers to use.
4. **Cost-efficient** Databend processes queries with high performance, and the user only pays for what is actually used.

## Design Overview

![Databend Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and its computing model is based on [apache-arrow](https://arrow.apache.org/).

Databend consists of three components: `meta service` layer, and the  decoupled `compute` and `storage` layers.

### Meta Service Layer

The meta service is a layer to service multiple tenants.
In current implementation, the meta service has components:
* **Metadata** - Which manages all metadata of databases, tables, clusters, the transaction, etc.
* **Administration** Which stores user info, user management, access control information, usage statistics, etc.
* **Security** Which performs authorization and authentication to protect the privacy of users' data.

### Compute Layer

The compute layer is the clusters that running computing workloads, each cluster have many nodes, each node has components:
* **Planner** - Builds execution plan from the user's SQL statement.
* **Optimizer** - Optimizer rules like predicate push down or pruning of unused columns.
* **Processors** - Vector-based query execution pipeline, which is build by planner instructions.
* **Cache** - Caching Data and Indexes based on the version.

Many clusters can attach the same database, so they can serve the query in parallel by different users.

### Storage Layer

Databend stores data in an efficient, columnar format as Parquet files.
For efficient pruning, Databend also creates indexes for each Parquet file to speed up the queries.

## Getting Started

* [Quick Start](https://databend.rs/overview/building-and-running/)
* [Whitepapers](https://databend.rs/overview/architecture/)
* [Performance](https://databend.rs/overview/performance/)
* [CLI Design](https://databend.rs/rfcs/cli/0001-cli-design/)
* [Contributing](https://databend.rs/development/contributing/)
* [Databend Weekly](https://datafuselabs.github.io/weekly/)

## Roadmap

Databend is currently in **Alpha** and is not ready to be used in production, [Roadmap 2021](https://github.com/datafuselabs/databend/issues/746)

## License

Databend is licensed under [Apache 2.0](LICENSE).
