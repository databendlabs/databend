<h1 align="center">The Future of Cloud Data Analytics</h1>

<div align="center">

<h4 align="center">
  <a href="https://databend.rs/doc/cloud">Databend Serverless Cloud (beta)</a>  |
  <a href="https://databend.rs/doc">Documentation</a>  |
  <a href="https://benchmark.clickhouse.com/">Benchmarking</a>  |
  <a href="https://github.com/datafuselabs/databend/issues/10334">Roadmap (v1.1)</a>

</h4>

<div>
<a href="https://link.databend.rs/join-slack">
<img src="https://img.shields.io/badge/slack-databend-0abd59?logo=slack" alt="slack" />
</a>

<a href="https://link.databend.rs/join-feishu">
<img src="https://img.shields.io/badge/feishu-databend-0abd59" alt="feishu" />
</a>

<br>

<a href="https://github.com/datafuselabs/databend/actions">
<img src="https://img.shields.io/github/actions/workflow/status/datafuselabs/databend/release.yml?branch=main" alt="CI Status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Linux Platform" />

<a href="https://opensource.org/licenses/Apache-2.0">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="license" />
</a>

</div>
</div>

<img src="https://user-images.githubusercontent.com/172204/219559279-ab7a11a4-9437-4a0a-85e8-cedf9ba0e24b.svg" alt="databend" />

## What is Databend?

Databend is an open-source **Elastic** and **Workload-Aware** modern cloud data warehouse focusing on Low-Cost and Low-Complexity for your massive-scale analytics needs.

Databend uses the latest techniques in vectorized query processing to allow you to do blazing-fast data analytics on object storage:
([S3](https://aws.amazon.com/s3/), [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/), [Google Cloud Storage](https://cloud.google.com/storage/), [Alibaba Cloud OSS](https://www.alibabacloud.com/product/object-storage-service), [Tencent Cloud COS](https://www.tencentcloud.com/products/cos), [Huawei Cloud OBS](https://www.huaweicloud.com/intl/en-us/product/obs.html), [Cloudflare R2](https://www.cloudflare.com/products/r2/), [Wasabi](https://wasabi.com/) or [MinIO](https://min.io)).

- __Feature-Rich__

  Support for atomic operations including `SELECT/INSERT/DELETE/UPDATE/REPLACE/COPY/ALTER` and advanced features like Time Travel, Multi Catalog(Apache Hive/Apache Iceberg).


- __Instant Elasticity__

  Databend completely separates storage from compute, which allows you easily scale up or scale down based on your application's needs.


- __Blazing Performance__

  Databend leverages data-level parallelism(Vectorized Query Execution) and instruction-level parallelism(SIMD) technology, offering [blazing performance](https://benchmark.clickhouse.com/) data analytics.


- __Git-like MVCC Storage__

  [Databend stores data with snapshots](https://databend.rs/doc/sql-commands/ddl/table/optimize-table#what-are-snapshot-segment-and-block), enabling users to effortlessly query, clone, or restore data from any history timepoint.


- __Support for Semi-Structured Data__

  Databend supports [ingestion of semi-structured data](https://databend.rs/doc/load-data) in various formats like CSV, JSON, and Parquet, which are located in the cloud or your local file system; Databend also supports semi-structured data types: [ARRAY, TUPLE, MAP, JSON](https://databend.rs/doc/sql-reference/data-types/data-type-semi-structured-types), which is easy to import and operate on semi-structured.


- __MySQL/ClickHouse Compatible__

  Databend is ANSI SQL compliant and MySQL/ClickHouse wire protocol compatible, making it easy to connect with existing tools([MySQL Client](https://databend.rs/doc/integrations/api/mysql-handler), [ClickHouse HTTP Handler](https://databend.rs/doc/integrations/api/clickhouse-handler), [Vector](https://vector.dev/), [DBeaver](https://dbeaver.com/), [Jupyter](https://databend.rs/doc/integrations/gui-tool/jupyter), [JDBC](https://databend.rs/doc/develop), etc.).


- __Easy to Use__

  Databend has no indexes to build, no manual tuning required, no manual figuring out partitions or shard data, it’s all done for you as data is loaded into the table.


## Architecture

![databend-arch](https://user-images.githubusercontent.com/172204/181448994-2b7c1623-6b20-4398-8917-45acca95ba90.png)


## Try Databend

### 1. Databend Serverless Cloud

The fastest way to try Databend, [Databend Cloud](https://databend.rs/doc/cloud/)

### 2. Install Databend from Docker

Prepare the image (once) from Docker Hub (this will download about 170 MB data):

```shell
docker pull datafuselabs/databend
```

To run Databend quickly:
```shell
docker run --net=host  datafuselabs/databend
```

## Getting Started

### Deploying Databend

- [Understanding Deployment Modes](https://databend.rs/doc/deploy/understanding-deployment-modes)
- [Deploying a Standalone Databend](https://databend.rs/doc/deploy/deploying-databend)
- [Expanding a Standalone Databend](https://databend.rs/doc/deploy/expanding-to-a-databend-cluster)
- [Databend Cloud (Beta)](https://databend.rs/doc/cloud)

### Connecting to Databend

- [How to Connect Databend with MySQL Client](https://databend.rs/doc/integrations/api/mysql-handler)
- [How to Connect Databend with ClickHouse HTTP Handler](https://databend.rs/doc/integrations/api/clickhouse-handler)
- [How to Connect Databend with DBeaver SQL IDE](https://databend.rs/doc/integrations/gui-tool/dbeaver)
- [How to Execute Queries in Python](https://databend.rs/doc/develop/python)
- [How to Query Databend in Jupyter Notebooks](https://databend.rs/doc/integrations/gui-tool/jupyter)
- [How to Execute Queries in Golang](https://databend.rs/doc/develop/golang)
- [How to Work with Databend in Node.js](https://databend.rs/doc/develop/nodejs)

### Loading Data into Databend

- [How to Load Data from Local File System](https://databend.rs/doc/load-data/local)
- [How to Load Data from Remote Files](https://databend.rs/doc/load-data/http)
- [How to Load Data from Amazon S3](https://databend.rs/doc/load-data/s3)
- [How to Load Data from Databend Stages](https://databend.rs/doc/load-data/stage)
- [Querying Data in Staged Files](https://databend.rs/doc/load-data/querying-stage)
- [Transforming Data During a Load](http://databend.rs/doc/load-data/data-load-transform)

### Unloading Data from Databend

- [How to Unload Data from Databend](https://databend.rs/doc/unload-data/)

### Managing Data

- [COPY](https://databend.rs/doc/sql-commands/dml/dml-copy-into-table)
- [INSERT](https://databend.rs/doc/sql-commands/dml/dml-insert)
- [DELETE](https://databend.rs/doc/sql-commands/dml/dml-delete-from)
- [UPDATE](https://databend.rs/doc/sql-commands/dml/dml-update)
- [REPLACE](https://databend.rs/doc/sql-commands/dml/dml-replace)

### Managing Users

- [How to Create a User](https://databend.rs/doc/sql-commands/ddl/user/user-create-user)
- [How to Grant Privileges to a User](https://databend.rs/doc/sql-commands/ddl/user/grant-privileges)
- [How to Revoke Privileges from a User](https://databend.rs/doc/sql-commands/ddl/user/revoke-privileges)
- [How to Create a Role](https://databend.rs/doc/sql-commands/ddl/user/user-create-role)
- [How to Grant Privileges to a Role](https://databend.rs/doc/sql-commands/ddl/user/grant-privileges)
- [How to Grant Role to a User](https://databend.rs/doc/sql-commands/ddl/user/grant-role)
- [How to Revoke Role from a User](https://databend.rs/doc/sql-commands/ddl/user/revoke-role)

### Managing Databases

- [How to Create a Database](https://databend.rs/doc/sql-commands/ddl/database/ddl-create-database)
- [How to Drop a Database](https://databend.rs/doc/sql-commands/ddl/database/ddl-drop-database)

### Managing Tables

- [How to Create a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-create-table)
- [How to Drop a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-drop-table)
- [How to Rename a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-rename-table)
- [How to Truncate a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-truncate-table)
- [How to Add/Drop Table Column](https://databend.rs/doc/sql-commands/ddl/table/alter-table-column)

### Managing Views

- [How to Create a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-create-view)
- [How to Drop a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-drop-view)
- [How to Alter a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-alter-view)


## AI Functions

- [Generating SQL with AI](https://databend.rs/doc/sql-functions/ai-functions/ai-to-sql)
- [Creating Embedding Vectors](https://databend.rs/doc/sql-functions/ai-functions/ai-embedding-vector)
- [Computing Text Similarities](https://databend.rs/doc/sql-functions/ai-functions/cosine-distance)
- [Text Completion with AI](https://databend.rs/doc/sql-functions/ai-functions/ai-text-completion)


### Managing User-Defined Functions

- [How to Create a User-Defined Function](https://databend.rs/doc/sql-commands/ddl/udf/ddl-create-function)
- [How to Drop a User-Defined Function](https://databend.rs/doc/sql-commands/ddl/udf/ddl-drop-function)
- [How to Alter a User-Defined Function](https://databend.rs/doc/sql-commands/ddl/udf/ddl-alter-function)

### Backup & Restore

- [How to Back Up Meta Data](https://databend.rs/doc/deploy/metasrv/metasrv-backup-restore)
- [How to Back Up Databases](https://databend.rs/doc/deploy/backup-restore/backup-and-restore-schema)

### Use Cases

- [Analyzing Nginx Access Logs With Databend](https://databend.rs/doc/use-cases/analyze-nginx-logs-with-databend-and-vector)
- [User Retention Analysis With Databend](https://databend.rs/doc/use-cases/analyze-user-retention-with-databend)
- [Conversion Funnel Analysis With Databend](https://databend.rs/doc/use-cases/analyze-funnel-with-databend)

### Performance

- [How to Benchmark Databend using TPC-H](https://databend.rs/blog/2022/08/08/benchmark-tpc-h)


## Contributing

Databend is an open source project, you can help with ideas, code, or documentation, we appreciate any efforts that help us to make the project better!
Once the code is merged, your name will be stored in the **system.contributors** table forever.

To get started with contributing:

- [Building Databend From Source](https://databend.rs/doc/contributing/building-from-source)
- [The First Good Pull Request](https://databend.rs/doc/contributing/good-pr)


## Community

For general help in using Databend, please refer to the official documentation. For additional help, you can use one of these channels to ask a question:

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Get the news fast)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)

## Roadmap

- [Roadmap v1.1](https://github.com/datafuselabs/databend/issues/10334)
- [Roadmap v1.0](https://github.com/datafuselabs/databend/issues/9604)
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)
- [Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)

## License

Databend is licensed under [Apache 2.0](LICENSE).

## Acknowledgement

- Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture).
- Databend's computing model is based on [Arrow2](https://github.com/jorgecarleitao/arrow2), Arrow2 is a faster and safer implementation of the Apache Arrow Columnar Format.
- The [documentation website](https://databend.rs) is hosted by [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
- Thanks to [Mergify](https://mergify.com/) for sponsoring advanced features like Batch Merge.
