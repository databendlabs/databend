<h1 align="center">Databend: The Next-Gen Cloud [Data+AI] Analytics</h1>

<div align="center">

<h4 align="center">
  <a href="https://databend.rs/doc/cloud">Databend Serverless Cloud (beta)</a>  |
  <a href="https://databend.rs/doc">Documentation</a>  |
  <a href="https://benchmark.clickhouse.com/">Benchmarking</a>  |
  <a href="https://github.com/datafuselabs/databend/issues/11868">Roadmap (v1.3)</a>

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

</div>
</div>

<h4 align="center">
<img src="https://github.com/datafuselabs/databend/assets/172204/0db97eea-7f25-48a6-b038-6d153378a6d4" width="600" alt="databend" />
</h4>

## 🚀 Why Databend?

**Databend** is an open-source, Elastic, Workload-Aware cloud data warehouse engineered for blazing-speed data analytics at a massive scale. Crafted with Rust, it's your most efficient alternative to Snowflake. In Databend, everything is distributed execution.

- **🌐Cloud-Agnostic**: Deploy on any cloud platform, including [S3](https://aws.amazon.com/s3/), [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/), [Google Cloud Storage](https://cloud.google.com/storage/), [Alibaba Cloud OSS](https://www.alibabacloud.com/product/object-storage-service), [Tencent Cloud COS](https://www.tencentcloud.com/products/cos), [Cloudflare R2](https://www.cloudflare.com/products/r2/), [Wasabi](https://wasabi.com/) or [MinIO](https://min.io).

- **⚡Performance-Driven**: Built with Rust and leveraging advanced technologies like SIMD and vectorized query processing, Databend ensures [exceptional analytic speeds](https://benchmark.clickhouse.com/), outperforming traditional cloud data warehouses.

- **📈Cost-Efficient Elasticity**: With its unique architecture, Databend decouples storage and computation. You can dynamically scale based on your needs, optimizing costs and performance.

- **🔄Simplified Data Management**: Say goodbye to traditional ETL complications. [Databend preprocesses data during ingestion](https://databend.rs/doc/load-data/load), simplifying your data flow.

- **🏡Lakehouse Design**: Combining data lake scalability with data warehouse efficiency, Databend smoothly integrates with Hive and Iceberg, granting the best of both domains.

- **🕰Snapshot and MVCC**: Tap into Git-like MVCC storage to conveniently query, clone, or revert data from any historical moment.

- **📑Rich Data Support**: Whether it's semi-structured data like JSON, CSV, or Parquet, or data types like ARRAY, TUPLE, or MAP, Databend handles them with ease.

- **🤖AI-Integrated Analytics**: Elevate your data analytics with integrated AI capabilities, opening new avenues for insights.

- **👥Open Source & Community-Driven**: With a growing community, Databend ensures transparency, constant updates, and an open platform for all your cloud analytics needs.

## 📐Architecture

![databend-arch](https://user-images.githubusercontent.com/172204/181448994-2b7c1623-6b20-4398-8917-45acca95ba90.png)


## 🚀 Try Databend

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

## 🚀Getting Started

<details>
<summary>Deploying Databend</summary>

- [Understanding Deployment Modes](https://databend.rs/doc/deploy/understanding-deployment-modes)
- [Deploying a Standalone Databend](https://databend.rs/doc/deploy/deploying-databend)
- [Expanding a Standalone Databend](https://databend.rs/doc/deploy/expanding-to-a-databend-cluster)
- [Databend Cloud (Beta)](https://databend.rs/doc/cloud)
</details>

<details>
<summary>Connecting to Databend</summary>

- [Connecting to Databend with BendSQL](https://databend.rs/doc/sql-clients/bendsql)
- [Connecting to Databend with JDBC](https://databend.rs/doc/sql-clients/jdbc)
- [Connecting to Databend with MySQL-Compatible Clients](https://databend.rs/doc/sql-clients/mysql)

</details>

<details>
<summary>Loading Data into Databend</summary>

- [How to Load Data from Local File](https://databend.rs/doc/load-data/load/local)
- [How to Load Data from Bucket](https://databend.rs/doc/load-data/load/s3)
- [How to Load Data from Stage](https://databend.rs/doc/load-data/load/stage)
- [How to Load Data from Remote Files](https://databend.rs/doc/load-data/load/http)
- [Querying Data in Staged Files](https://databend.rs/doc/load-data/transform/querying-stage)
- [Transforming Data During a Load](https://databend.rs/doc/load-data/transform/data-load-transform)
- [How to Unload Data from Databend](https://databend.rs/doc/load-data/unload)

</details>

<details>
<summary>Loading Data Tools with Databend</summary>

- [Apache Kafka](https://databend.rs/doc/load-data/load-db/kafka)
- [Airbyte](https://databend.rs/doc/load-data/load-db/airbyte)
- [dbt](https://databend.rs/doc/load-data/load-db/dbt)
- [Debezium](https://databend.rs/doc/load-data/load-db/debezium)
- [Apache Flink CDC](https://databend.rs/doc/load-data/load-db/flink-cdc)
- [DataDog Vector](https://databend.rs/doc/load-data/load-db/vector)
- [Addax](https://databend.rs/doc/load-data/load-db/addax)
- [DataX](https://databend.rs/doc/load-data/load-db/datax)

</details>

<details>
<summary>Visualize Tools with Databend</summary>

- [Metabase](https://databend.rs/doc/integrations/metabase)
- [Tableau](https://databend.rs/doc/integrations/tableau)
- [Grafana](https://databend.rs/doc/integrations/grafana)
- [Jupyter Notebook](https://databend.rs/doc/integrations/jupyter)
- [Deepnote](https://databend.rs/doc/integrations/deepnote)
- [MindsDB](https://databend.rs/doc/integrations/mindsdb)
- [Redash](https://databend.rs/doc/integrations/redash)

</details>



<details>
<summary>Managing Users</summary>

- [How to Create a User](https://databend.rs/doc/sql-commands/ddl/user/user-create-user)
- [How to Grant Privileges to a User](https://databend.rs/doc/sql-commands/ddl/user/grant-privileges)
- [How to Revoke Privileges from a User](https://databend.rs/doc/sql-commands/ddl/user/revoke-privileges)
- [How to Create a Role](https://databend.rs/doc/sql-commands/ddl/user/user-create-role)
- [How to Grant Privileges to a Role](https://databend.rs/doc/sql-commands/ddl/user/grant-privileges)
- [How to Grant Role to a User](https://databend.rs/doc/sql-commands/ddl/user/grant-role)
- [How to Revoke Role from a User](https://databend.rs/doc/sql-commands/ddl/user/revoke-role)
</details>

<details>
<summary>Managing Databases</summary>

- [How to Create a Database](https://databend.rs/doc/sql-commands/ddl/database/ddl-create-database)
- [How to Drop a Database](https://databend.rs/doc/sql-commands/ddl/database/ddl-drop-database)
</details>

<details>
<summary>Managing Tables</summary>

- [How to Create a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-create-table)
- [How to Drop a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-drop-table)
- [How to Rename a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-rename-table)
- [How to Truncate a Table](https://databend.rs/doc/sql-commands/ddl/table/ddl-truncate-table)
- [How to Flash Back a Table](https://databend.rs/doc/sql-commands/ddl/table/flashback-table)
- [How to Add/Drop Table Column](https://databend.rs/doc/sql-commands/ddl/table/alter-table-column)
</details>

<details>
<summary>Managing Data</summary>

- [COPY](https://databend.rs/doc/sql-commands/dml/dml-copy-into-table)
- [INSERT](https://databend.rs/doc/sql-commands/dml/dml-insert)
- [DELETE](https://databend.rs/doc/sql-commands/dml/dml-delete-from)
- [UPDATE](https://databend.rs/doc/sql-commands/dml/dml-update)
- [REPLACE](https://databend.rs/doc/sql-commands/dml/dml-replace)
</details>

<details>
<summary>Managing Views</summary>

- [How to Create a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-create-view)
- [How to Drop a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-drop-view)
- [How to Alter a View](https://databend.rs/doc/sql-commands/ddl/view/ddl-alter-view)
</details>

<details>
<summary>AI Functions</summary>

- [Generating SQL with AI](https://databend.rs/doc/sql-functions/ai-functions/ai-to-sql)
- [Creating Embedding Vectors](https://databend.rs/doc/sql-functions/ai-functions/ai-embedding-vector)
- [Computing Text Similarities](https://databend.rs/doc/sql-functions/ai-functions/ai-cosine-distance)
- [Text Completion with AI](https://databend.rs/doc/sql-functions/ai-functions/ai-text-completion)
</details>

<details>
<summary>Data Governance</summary>

- [How to Create Data Masking Policy](https://databend.rs/doc/sql-commands/ddl/mask-policy/create-mask-policy)
- [How to Drop Data Masking Policy](https://databend.rs/doc/sql-commands/ddl/mask-policy/drop-mask-policy)

</details>

<details>
<summary>Securing Databend</summary>

- [How to Create Network Policy](https://databend.rs/doc/sql-commands/ddl/network-policy/ddl-create-policy)
- [How to Drop Network Policy](https://databend.rs/doc/sql-commands/ddl/network-policy/ddl-drop-policy)
- [How to Alter Network Policy](https://databend.rs/doc/sql-commands/ddl/network-policy/ddl-alter-policy)

</details>


<details>
<summary>Performance</summary>

- [How to Benchmark Databend using TPC-H](https://databend.rs/blog/2022/08/08/benchmark-tpc-h)
</details>


## 🤝 Contributing

Databend thrives on community contributions! Whether it's through ideas, code, or documentation, every effort helps in enhancing our project. As a token of our appreciation, once your code is merged, your name will be eternally preserved in the **system.contributors** table.

Here are some resources to help you get started:

- [Building Databend From Source](https://databend.rs/doc/contributing/building-from-source)
- [The First Good Pull Request](https://databend.rs/doc/contributing/good-pr)


## 👥Community

For guidance on using Databend, we recommend starting with the official documentation. If you need further assistance, explore the following community channels:

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Get the news fast)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)


## 🛣️ Roadmap

Stay updated with Databend's development journey. Here are our roadmap milestones:

- [Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)
- [Roadmap v1.0](https://github.com/datafuselabs/databend/issues/9604)
- [Roadmap v1.1](https://github.com/datafuselabs/databend/issues/10334)
- [Roadmap v1.2](https://github.com/datafuselabs/databend/issues/11073)
- [Roadmap v1.3](https://github.com/datafuselabs/databend/issues/11868)
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)


## 📜License

Databend is released under a combination of two licenses: the [Apache License 2.0](licenses/Apache-2.0.txt) and the [Elastic License 2.0](licenses/Elastic.txt).

When contributing to Databend, you can find the relevant license header in each file.

For more information, see the [LICENSE](LICENSE) file and [Licensing FAQs](https://databend.rs/doc/enterprise/license).


## 🙏Acknowledgement

- **Inspiration**: Databend's design draws inspiration from industry leaders [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture).

- **Computing Model**: Our computing foundation is built upon [Arrow2](https://github.com/jorgecarleitao/arrow2), a faster and more secure rendition of the Apache Arrow Columnar Format.

- **Documentation Hosting**: The [Databend documentation website](https://databend.rs) proudly runs on [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
