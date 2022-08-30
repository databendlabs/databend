<img src="https://repository-images.githubusercontent.com/302827809/a01c8064-0196-45d9-b326-1762d6d3062b" alt="databend" />
<div align="center">
 
<h4 align="center">
  <a href="https://databend.rs/doc/cloud">Databend Serverless Cloud (beta)</a>  |
  <a href="https://databend.rs/doc">Documentation</a>  |
  <a href="https://perf.databend.rs">Benchmarking</a>  |
  <a href="https://github.com/datafuselabs/databend/issues/7052">Roadmap (v0.9)</a>

</h4>

<div>
<a href="https://link.databend.rs/join-slack">
<img src="https://badgen.net/badge/Slack/Join%20Databend/0abd59?icon=slack" alt="slack" />
</a>

<a href="https://github.com/datafuselabs/databend/actions">
<img src="https://img.shields.io/github/workflow/status/datafuselabs/databend/Release" alt="CI Status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Linux Platform" />

<a href="https://opensource.org/licenses/Apache-2.0">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="license" />
</a>

</div>
</div>
<br>

- [What is Databend?](#what-is-databend)
- [Architecture](#architecture)
- [Try Databend](#try-databend)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [Community](#community)
- [Roadmap](#roadmap)

## What is Databend?

Databend is an open-source **Elastic** and **Workload-Aware** modern cloud data warehouse.

Databend uses the latest techniques in vectorized query processing to allow you to do blazing-fast data analytics on object storage([S3](https://aws.amazon.com/s3/), [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/), [Google Cloud Storage](https://cloud.google.com/storage/) or [MinIO](https://min.io)).

- __Instant Elasticity__

  Databend completely separates storage from compute, which allows you easily scale up or scale down based on your application's needs.

- __Blazing Performance__

  Databend leverages data-level parallelism(Vectorized Query Execution) and instruction-level parallelism(SIMD) technology, offering blazing performance data analytics.
  
  
- __Git-like Storage__

  Databend stores data with snapshots. It's easy to query, clone, and restore historical data in tables.

- __Support for Semi-Structured Data__

  Databend supports [ingestion of semi-structured data](https://databend.rs/doc/load-data) in various formats like CSV, JSON, and Parquet, which are located in the cloud or your local file system; Databend also supports semi-structured data types: [ARRAY, MAP, JSON](https://databend.rs/doc/reference/data-types/data-type-semi-structured-types), which is easy to import and operate on semi-structured.

- __MySQL/ClickHouse Compatible__

  Databend is ANSI SQL compliant and MySQL/ClickHouse wire protocol compatible, making it easy to connect with existing tools([MySQL Client](https://databend.rs/doc/reference/api/mysql-handler), [ClickHouse Client](https://databend.rs/doc/reference/api/clickhouse-handler), [Vector](https://vector.dev/), [DBeaver](https://dbeaver.com/), [Jupyter](https://databend.rs/doc/integrations/gui-tool/jupyter), [JDBC](https://databend.rs/doc/develop), etc.).

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

- [How to Connect Databend with MySQL Client](https://databend.rs/doc/reference/api/mysql-handler)
- [How to Connect Databend with ClickHouse Client](https://databend.rs/doc/reference/api/clickhouse-handler)
- [How to Connect Databend with DBeaver SQL IDE](https://databend.rs/doc/integrations/gui-tool/dbeaver)
- [How to Execute Queries in Python](https://databend.rs/doc/develop/python)
- [How to Query Databend in Jupyter Notebooks](https://databend.rs/doc/integrations/gui-tool/jupyter)
- [How to Execute Queries in Golang](https://databend.rs/doc/develop/golang)
- [How to Work with Databend in Node.js](https://databend.rs/doc/develop/nodejs)

### Loading Data into Databend

- [How to Load Data from Local File System](https://databend.rs/doc/load-data/local)
- [How to Load Data from Remote Files](https://databend.rs/doc/load-data/remote)
- [How to Load Data from Amazon S3](https://databend.rs/doc/load-data/s3)
- [How to Load Data from Databend Stages](https://databend.rs/doc/load-data/stage)
- [How to Load Data from MySQL](https://databend.rs/doc/load-data/mysql)

### Managing Users

- [How to Create a User](https://databend.rs/doc/reference/sql/ddl/user/user-create-user)
- [How to Grant Privileges to a User](https://databend.rs/doc/reference/sql/ddl/user/grant-privileges)
- [How to Revoke Privileges from a User](https://databend.rs/doc/reference/sql/ddl/user/revoke-privileges)
- [How to Create a Role](https://databend.rs/doc/reference/sql/ddl/user/user-create-role)
- [How to Grant Privileges to a Role](https://databend.rs/doc/reference/sql/ddl/user/grant-privileges)
- [How to Grant Role to a User](https://databend.rs/doc/reference/sql/ddl/user/grant-role)
- [How to Revoke Role from a User](https://databend.rs/doc/reference/sql/ddl/user/revoke-role)
 
### Managing Databases

- [How to Create a Database](https://databend.rs/doc/reference/sql/ddl/database/ddl-create-database)
- [How to Drop a Database](https://databend.rs/doc/reference/sql/ddl/database/ddl-drop-database)

### Managing Tables

- [How to Create a Table](https://databend.rs/doc/reference/sql/ddl/table/ddl-create-table)
- [How to Drop a Table](https://databend.rs/doc/reference/sql/ddl/table/ddl-drop-table)
- [How to Rename a Table](https://databend.rs/doc/reference/sql/ddl/table/ddl-rename-table)
- [How to Truncate a Table](https://databend.rs/doc/reference/sql/ddl/table/ddl-truncate-table)

### Managing Views

- [How to Create a View](https://databend.rs/doc/reference/sql/ddl/view/ddl-create-view)
- [How to Drop a View](https://databend.rs/doc/reference/sql/ddl/view/ddl-drop-view)
- [How to Alter a View](https://databend.rs/doc/reference/sql/ddl/view/ddl-alter-view)

### Managing User-Defined Functions

- [How to Create a User-Defined Function](http://databend.rs/doc/reference/sql/ddl/udf/ddl-create-function)
- [How to Drop a User-Defined Function](http://databend.rs/doc/reference/sql/ddl/udf/ddl-drop-function)
- [How to Alter a User-Defined Function](http://databend.rs/doc/reference/sql/ddl/udf/ddl-alter-function)

### Backup & Restore

* [How to Back Up Meta Data](https://databend.rs/doc/manage/metasrv/metasrv-backup-restore)
* [How to Back Up Databases](https://databend.rs/doc/manage/backup-restore/backup-and-restore-schema)

### Use Cases

- [Analyzing Nginx Access Logs With Databend](https://databend.rs/doc/learn/analyze-nginx-logs-with-databend-and-vector)
- [User Retention Analysis With Databend](https://databend.rs/doc/learn/analyze-user-retention-with-databend)
- [Conversion Funnel Analysis With Databend](https://databend.rs/doc/learn/analyze-funnel-with-databend)

### Performance

- [How to Benchmark Databend](https://databend.rs/doc/learn/analyze-ontime-with-databend-on-ec2-and-s3)


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
- [Twitter](https://twitter.com/Datafuse_Labs) (Get the news fast)
- [Weekly](https://weekly.databend.rs/) (A weekly newsletter about Databend)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)

## Roadmap
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)
- [Roadmap v0.8](https://github.com/datafuselabs/databend/issues/4591)
- [Roadmap 2022](https://github.com/datafuselabs/databend/issues/3706)

## License

Databend is licensed under [Apache 2.0](LICENSE).

## Acknowledgement

- Databend is inspired by [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture), its computing model is based on [apache-arrow](https://arrow.apache.org/).
- The [documentation website](https://databend.rs) is hosted by [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
- Thanks to [Mergify](https://mergify.com/) for sponsoring advanced features like Batch Merge.
- Thanks to [QingCloud](https://qingcloud.com) for sponsoring CI resources.
