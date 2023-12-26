<h1 align="center">Databend: The Next-Gen Cloud [Data+AI] Analytics</h1>

<div align="center">

<h4 align="center">
  <a href="https://docs.databend.com/guides/cloud">Databend Serverless Cloud (beta)</a>  |
  <a href="https://docs.databend.com/">Documentation</a>  |
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

<a href="https://github.com/datafuselabs/databend/actions/workflows/release.yml">
<img src="https://img.shields.io/github/actions/workflow/status/datafuselabs/databend/release.yml?branch=main" alt="CI Status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Linux Platform" />

</div>
</div>

<img src="https://github.com/datafuselabs/databend/assets/172204/9997d8bc-6462-4dbd-90e3-527cf50a709c" alt="databend" />

## 🐋 Introduction

**Databend** is an open-source, elastic, and workload-aware cloud data warehouse built in Rust, offering a cost-effective [alternative to Snowflake](https://github.com/datafuselabs/databend/issues/13059). It's designed for complex analysis of the world's largest datasets.

## 🚀 Why Databend

- **Cloud-Friendly**: Seamlessly integrates with various cloud storages like AWS S3, Azure Blob, Google Cloud, and more.

- **High Performance**: Built in Rust, utilizing SIMD and vectorized processing for rapid analytics. [See ClickBench](https://databend.com/blog/clickbench-databend-top).

- **Cost-Efficient Elasticity**: Innovative design for separate scaling of storage and computation, optimizing both costs and performance.

- **Easy Data Management**: Integrated data preprocessing during ingestion eliminates the need for external ETL tools.

- **Data Version Control**: Offers Git-like multi-version storage, enabling easy data querying, cloning, and reverting from any point in time.

- **Rich Data Support**: Handles diverse data formats and types, including JSON, CSV, Parquet, ARRAY, TUPLE, MAP, and JSON.

- **AI-Enhanced Analytics**: Offers advanced analytics capabilities with integrated [AI Functions](https://docs.databend.com/sql/sql-functions/ai-functions/).

- **Community-Driven**: Benefit from a friendly, growing community that offers an easy-to-use platform for all your cloud analytics.

## 📐 Architecture

![Databend Architecture](https://github.com/datafuselabs/databend/assets/172204/68b1adc6-0ec1-41d4-9e1d-37b80ce0e5ef)

## 🚀 Try Databend

### 1. Databend Serverless Cloud

The fastest way to try Databend, [Databend Cloud](https://databend.com)

### 2. Install Databend from Docker

Prepare the image (once) from Docker Hub (this will download about 170 MB data):

```shell
docker pull datafuselabs/databend
```

To run Databend quickly:
```shell
docker run --net=host  datafuselabs/databend
```

## 🚀 Getting Started

<details>
<summary>Deploying Databend</summary>

- [Understanding Deployment Modes](https://docs.databend.com/guides/deploy/understanding-deployment-modes)
- [Deploying a Standalone Databend](https://docs.databend.com/guides/deploy/deploying-databend)
- [Expanding a Standalone Databend](https://docs.databend.com/guides/deploy/expanding-to-a-databend-cluster)
- [Databend Cloud (Beta)](https://docs.databend.com/guides/cloud)
</details>

<details>
<summary>Connecting to Databend</summary>

- [Connecting to Databend with BendSQL](https://docs.databend.com/guides/sql-clients/bendsql)
- [Connecting to Databend with JDBC](https://docs.databend.com/guides/sql-clients/jdbc)

</details>

<details>
<summary>Loading Data into Databend</summary>

- [How to Load Data from Local File](https://docs.databend.com/guides/load-data/load/local)
- [How to Load Data from Bucket](https://docs.databend.com/guides/load-data/load/s3)
- [How to Load Data from Stage](https://docs.databend.com/guides/load-data/load/stage)
- [How to Load Data from Remote Files](https://docs.databend.com/guides/load-data/load/http)
- [Querying Data in Staged Files](https://docs.databend.com/guides/load-data/transform/querying-stage)
- [Transforming Data During a Load](https://docs.databend.com/guides/load-data/transform/data-load-transform)
- [How to Unload Data from Databend](https://docs.databend.com/guides/unload-data/)

</details>

<details>
<summary>Loading Data Tools with Databend</summary>

- [Apache Kafka](https://docs.databend.com/guides/load-data/load-db/kafka)
- [Airbyte](https://docs.databend.com/guides/load-data/load-db/airbyte)
- [dbt](https://docs.databend.com/guides/load-data/load-db/dbt)
- [Debezium](https://docs.databend.com/guides/load-data/load-db/debezium)
- [Apache Flink CDC](https://docs.databend.com/guides/load-data/load-db/flink-cdc)
- [DataDog Vector](https://docs.databend.com/guides/load-data/load-db/vector)
- [Addax](https://docs.databend.com/guides/load-data/load-db/addax)
- [DataX](https://docs.databend.com/guides/load-data/load-db/datax)

</details>

<details>
<summary>Visualize Tools with Databend</summary>

- [Metabase](https://docs.databend.com/guides/visualize/metabase)
- [Tableau](https://docs.databend.com/guides/visualize/tableau)
- [Grafana](https://docs.databend.com/guides/visualize/grafana)
- [Jupyter Notebook](https://docs.databend.com/guides/visualize/jupyter)
- [Deepnote](https://docs.databend.com/guides/visualize/deepnote)
- [MindsDB](https://docs.databend.com/guides/visualize/mindsdb)
- [Redash](https://docs.databend.com/guides/visualize/redash)

</details>

<details>
<summary>Managing Users</summary>

- [How to Create a User](https://docs.databend.com/sql/sql-commands/ddl/user/user-create-user)
- [How to Grant Privileges to a User](https://docs.databend.com/sql/sql-commands/ddl/user/grant-privileges)
- [How to Revoke Privileges from a User](https://docs.databend.com/sql/sql-commands/ddl/user/revoke-privileges)
- [How to Create a Role](https://docs.databend.com/sql/sql-commands/ddl/user/user-create-role)
- [How to Grant Privileges to a Role](https://docs.databend.com/sql/sql-commands/ddl/user/grant-privileges)
- [How to Grant Role to a User](https://docs.databend.com/sql/sql-commands/ddl/user/grant-role)
- [How to Revoke the Role of a User](https://docs.databend.com/sql/sql-commands/ddl/user/revoke-role)
</details>

<details>
<summary>Managing Databases</summary>

- [How to Create a Database](https://docs.databend.com/sql/sql-commands/ddl/database/ddl-create-database)
- [How to Drop a Database](https://docs.databend.com/sql/sql-commands/ddl/database/ddl-drop-database)
</details>

<details>
<summary>Managing Tables</summary>

- [How to Create a Table](https://docs.databend.com/sql/sql-commands/ddl/table/ddl-create-table)
- [How to Drop a Table](https://docs.databend.com/sql/sql-commands/ddl/table/ddl-drop-table)
- [How to Rename a Table](https://docs.databend.com/sql/sql-commands/ddl/table/ddl-rename-table)
- [How to Truncate a Table](https://docs.databend.com/sql/sql-commands/ddl/table/ddl-truncate-table)
- [How to Flash Back a Table](https://docs.databend.com/sql/sql-commands/ddl/table/flashback-table)
- [How to Add/Drop Table Column](https://docs.databend.com/sql/sql-commands/ddl/table/alter-table-column)
</details>

<details>
<summary>Managing Data</summary>

- [COPY](https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table)
- [INSERT](https://docs.databend.com/sql/sql-commands/dml/dml-insert)
- [DELETE](https://docs.databend.com/sql/sql-commands/dml/dml-delete-from)
- [UPDATE](https://docs.databend.com/sql/sql-commands/dml/dml-update)
- [REPLACE](https://docs.databend.com/sql/sql-commands/dml/dml-replace)
</details>

<details>
<summary>Managing Views</summary>

- [How to Create a View](https://docs.databend.com/sql/sql-commands/ddl/view/ddl-create-view)
- [How to Drop a View](https://docs.databend.com/sql/sql-commands/ddl/view/ddl-drop-view)
- [How to Alter a View](https://docs.databend.com/sql/sql-commands/ddl/view/ddl-alter-view)
</details>

<details>
<summary>AI Functions</summary>

- [Generating SQL with AI](https://docs.databend.com/sql/sql-functions/ai-functions/ai-to-sql)
- [Creating Embedding Vectors](https://docs.databend.com/sql/sql-functions/ai-functions/ai-embedding-vector)
- [Computing Text Similarities](https://docs.databend.com/sql/sql-functions/ai-functions/ai-cosine-distance)
- [Text Completion with AI](https://docs.databend.com/sql/sql-functions/ai-functions/ai-text-completion)
</details>

<details>
<summary>Data Governance</summary>

- [How to Create Data Masking Policy](https://docs.databend.com/sql/sql-commands/ddl/mask-policy/create-mask-policy)
- [How to Drop Data Masking Policy](https://docs.databend.com/sql/sql-commands/ddl/mask-policy/drop-mask-policy)

</details>

<details>
<summary>Securing Databend</summary>

- [How to Create Network Policy](https://docs.databend.com/sql/sql-commands/ddl/network-policy/ddl-create-policy)
- [How to Drop Network Policy](https://docs.databend.com/sql/sql-commands/ddl/network-policy/ddl-drop-policy)
- [How to Alter Network Policy](https://docs.databend.com/sql/sql-commands/ddl/network-policy/ddl-alter-policy)

</details>

<details>
<summary>Performance</summary>
  
- [Review Clickbench](https://databend.com/blog/clickbench-databend-top)
- [How to Benchmark Databend using TPC-H](https://databend.com/blog/2022/08/08/benchmark-tpc-h)
  
</details>


## 🤝 Contributing

Databend thrives on community contributions! Whether it's through ideas, code, or documentation, every effort helps in enhancing our project. As a token of our appreciation, once your code is merged, your name will be eternally preserved in the **system.contributors** table.

Here are some resources to help you get started:

- [Building Databend From Source](https://docs.databend.com/guides/overview/community/contributor/building-from-source)
- [The First Good Pull Request](https://docs.databend.com/guides/overview/community/contributor/good-pr)


## 👥 Community

For guidance on using Databend, we recommend starting with the official documentation. If you need further assistance, explore the following community channels:

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs/) (Get the news fast)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)


## 🛣️ Roadmap

Stay updated with Databend's development journey. Here are our roadmap milestones:

- [Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)
- [Roadmap v1.3](https://github.com/datafuselabs/databend/issues/11868)
- [Roadmap v1.2](https://github.com/datafuselabs/databend/issues/11073)
- [Roadmap v1.1](https://github.com/datafuselabs/databend/issues/10334)
- [Roadmap v1.0](https://github.com/datafuselabs/databend/issues/9604)
- [Roadmap v0.9](https://github.com/datafuselabs/databend/issues/7052)


## 📜 License

Databend is released under a combination of two licenses: the [Apache License 2.0](licenses/Apache-2.0.txt) and the [Elastic License 2.0](licenses/Elastic.txt).

When contributing to Databend, you can find the relevant license header in each file.

For more information, see the [LICENSE](LICENSE) file and [Licensing FAQs](https://docs.databend.com/guides/overview/editions/dee/license).


## 🙏 Acknowledgement

- **Inspiration**: Databend's design draws inspiration from industry leaders [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture).

- **Computing Model**: Our computing foundation is built upon [Arrow2](https://github.com/jorgecarleitao/arrow2), a faster and more secure rendition of the Apache Arrow Columnar Format.

- **Documentation Hosting**: The [Databend documentation website](https://docs.databend.com) proudly runs on [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
