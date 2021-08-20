<div align="center">
<h1>Datafuse</h1>
<a href="https://datafuse.rs" target="_blank">
 <strong>The Open Source Cloud Warehouse for Everyone</strong>
 </a>
<br>
<br>

<div>
<a href="https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA">
<img src="https://badgen.net/badge/Slack/Join%20Datafuse/0abd59?icon=slack" alt="slack" />
</a>

<a href="https://github.com/datafuselabs/datafuse/actions">
<img src="https://github.com/datafuselabs/datafuse/actions/workflows/unit-tests.yml/badge.svg" alt="CI Status" />
</a>

<a href="https://codecov.io/gh/datafuselabs/datafuse">
<img src="https://codecov.io/gh/datafuselabs/datafuse/branch/master/graph/badge.svg" alt="codecov" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux,%20ARM,%20OS%20X,%20Windows-green.svg?style=flat" alt="patform" />

<a href="https://opensource.org/licenses/Apache-2.0">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="license" />
</a>

</div>
</div>
<br>

- [What is Datafuse?](#what-is-datafuse)
- [Design Overview](#design-overview)
   - [Meta Service Layer](#meta-service-layer)
   - [Compute Layer](#compute-layer)
   - [Storage Layer](#storage-layer)
- [Getting Started](#getting-started)
- [Roadmap](#roadmap)

## What is Datafuse

Datafuse is an open source **elastic** and **scalable** cloud warehouse, it offers blazing fast query and combines elasticity, simplicity, low cost of the cloud, built to make the Data Cloud easy.

We design Datafuse with the following key functionalities in mind:
1. **Elastic**  In Datafuse storage and compute resources can dynamically scale up and down on demand.
2. **Secure** All data files and network traffic in Datafuse is encrypted end-to-end, and provide Role Based Access Control in SQL level.
3. **User-friendly** Datafuse is an ANSI SQL compliant cloud warehouse, it is easy for data scientist and engineers to use.
4. **Cost-efficient** Datafuse processes queries with high performance, and the user only pays for what is actually used.

## Design Overview

![Datafuse Architecture](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/arch/datafuse-arch-20210817.svg)

The picture above shows the high-level architecture of Datafuse, it consists of three components: `meta service` layer, and the  decoupled `compute` and `storage` layers.

### Meta Service Layer

The meta service is a layer to service multiple tenants.
In current implementation, the meta service has components:
* Metadata, which manages all metadata of databases, tables, clusters, the transaction, etc.
* Administration, which stores user info, user management, access control information, usage statistics, etc.
* Security, which performs authorization and authentication to protect the privacy of users' data.

### Compute Layer

The compute layer is the layer to carry out computation for query processing. This layer may consist of many clusters,
and each cluster may consist of many nodes. Each node is a compute unit, and is a collection of components:
* Planner 
* Optimizer
* Processors
* Cache

Node is the smallest unit of the compute layer, they can be registered as one cluster via namespace.
Many clusters can attach the same database, so they can serve the query in parallel by different users.

### Storage Layer

Datafuse stores data in an efficient, columnar format as Parquet files.
Each Parquet file is sorted by the primary key before being written to the underlying shared storage.
For efficient pruning, Datafuse also creates indexes for each Parquet file to speed up the queries.

## Getting Started

* [Quick Start](https://datafuse.rs/overview/building-and-running/)
* [Whitepapers](https://datafuse.rs/overview/architecture/)
* [Performance](https://datafuse.rs/overview/performance/)
* [CLI Design](https://datafuse.rs/rfcs/cli/0001-cli-design/)
* [Contributing](https://datafuse.rs/development/contributing/)
* [Datafuse Weekly](https://datafuselabs.github.io/weekly/)

## Roadmap

Datafuse is currently in **Alpha** and is not ready to be used in production, [Roadmap 2021](https://github.com/datafuselabs/datafuse/issues/746)

## License

Datafuse is licensed under [Apache 2.0](LICENSE).
