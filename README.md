<h1 align="center">Databend: The Next-Gen Cloud [Data+AI] Analytics</h1>
<h2 align="center">The open-source, on-premise alternative to Snowflake</h2>

<div align="center">

<h4 align="center">
  <a href="https://docs.databend.com/guides/cloud">Databend Serverless Cloud (beta)</a>  |
  <a href="https://docs.databend.com/">Documentation</a>  |
  <a href="https://benchmark.clickhouse.com/">Benchmarking</a>  |
  <a href="https://github.com/databendlabs/databend/issues/11868">Roadmap (v1.3)</a>

</h4>

<div>
<a href="https://link.databend.com/join-slack">
<img src="https://img.shields.io/badge/slack-databend-0abd59?logo=slack" alt="slack" />
</a>

<a href="https://link.databend.com/join-feishu">
<img src="https://img.shields.io/badge/feishu-databend-0abd59" alt="feishu" />
</a>

<br>

<a href="https://github.com/databendlabs/databend/actions/workflows/release.yml">
<img src="https://img.shields.io/github/actions/workflow/status/datafuselabs/databend/release.yml?branch=main" alt="CI Status" />
</a>

<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Linux Platform" />

<a href="https://gurubase.io/g/databend">
<img src="https://img.shields.io/badge/Gurubase-Ask%20Databend%20Guru-006BFF" alt="Gurubase" />
</a>

</div>
</div>

<img src="https://github.com/databendlabs/databend/assets/172204/9997d8bc-6462-4dbd-90e3-527cf50a709c" alt="databend" />

## 🐋 Introduction

**Databend**, built in Rust, is an open-source cloud data warehouse that serves as a cost-effective [alternative to Snowflake](https://github.com/databendlabs/databend/issues/13059). With its focus on fast query execution and data ingestion, it's designed for complex analysis of the world's largest datasets.

**Production-Proven Scale:**
- 🤝 **Enterprise Adoption**: Trusted by over **50 organizations** processing more than **100 million queries daily**
- 🗄️ **Massive Scale**: Successfully managing over **800 petabytes** of analytical data

## ⚡ Performance

<div align="center">

[TPC-H Benchmark: Databend Cloud vs. Snowflake](https://docs.databend.com/guides/benchmark/tpch)

</div>

![Databend vs. Snowflake](https://github.com/databendlabs/wizard/assets/172204/d796acf0-0a66-4b1d-8754-cd2cd1de04c7)

<div align="center">

[Data Ingestion Benchmark: Databend Cloud vs. Snowflake](https://docs.databend.com/guides/benchmark/data-ingest)

</div>

![Databend vs. Snowflake](https://github.com/databendlabs/databend/assets/172204/c61d7a40-f6fe-4fb9-83e8-06ea9599aeb4)


## 🚀 Why Databend

- **Full Control**: Deploy on **cloud** or **on-prem** to suit your needs.

- **Blazing-Fast Performance**: Built with **Rust** for high-speed query execution. 👉 [ClickBench](https://databend.com/blog/clickbench-databend-top)

- **Cost-Effective**: Scalable architecture that boosts **performance** and reduces **costs**. 👉 [TPC-H](https://docs.databend.com/guides/benchmark/tpch)

- **AI-Enhanced Analytics**: Leverage built-in **[AI Functions](https://docs.databend.com/guides/ai-functions/)** for smarter data insights.

- **Simplified ETL**: Direct **data ingestion** without the need for external ETL tools. 👉 [Data Loading](https://docs.databend.com/guides/load-data/)

- **Real-Time Data Updates**: Keep your analytics **up-to-date** with real-time incremental data updates. 👉 [Stream](https://docs.databend.com/guides/load-data/continuous-data-pipelines/stream)

- **Advanced Indexing**: Boost query performance with **[Virtual Column](https://docs.databend.com/guides/performance/virtual-column)**, **[Aggregating Index](https://docs.databend.com/guides/performance/aggregating-index)**, and **[Full-Text Index](https://docs.databend.com/guides/performance/fulltext-index)**.

- **ACID Compliance + Version Control**: Ensure reliable **transactions** with full ACID compliance and Git-like versioning.

- **Schema Flexibility**: Effortlessly handle **semi-structured data** with the flexible **[VARIANT](https://docs.databend.com/sql/sql-reference/data-types/variant)** data type.

- **Community-Driven Growth**: **Open-source** and continuously evolving with contributions from a global community.



## 📐 Architecture

![Databend Architecture](https://github.com/databendlabs/databend/assets/172204/68b1adc6-0ec1-41d4-9e1d-37b80ce0e5ef)

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
<summary>Connecting to Databend</summary>

- [Connecting to Databend with BendSQL](https://docs.databend.com/guides/sql-clients/bendsql)
- [Connecting to Databend with JDBC](https://docs.databend.com/guides/sql-clients/jdbc)

</details>

<details>
<summary>Data Import and Export</summary>

- [How to load Parquet file into a table](https://docs.databend.com/guides/load-data/load-semistructured/load-parquet)
- [How to export a table to Parquet file](https://docs.databend.com/guides/unload-data/unload-parquet)
- [How to load CSV file into a table](https://docs.databend.com/guides/load-data/load-semistructured/load-csv)
- [How to export a table to CSV file](https://docs.databend.com/guides/unload-data/unload-csv)
- [How to load TSV file into a table](https://docs.databend.com/guides/load-data/load-semistructured/load-tsv)
- [How to export a table to TSV file](https://docs.databend.com/guides/unload-data/unload-tsv)
- [How to load NDJSON file into a table](https://docs.databend.com/guides/load-data/load-semistructured/load-ndjson)
- [How to export a table to NDJSON file](https://docs.databend.com/guides/unload-data/unload-ndjson)
- [How to load ORC file into a table](https://docs.databend.com/guides/load-data/load-semistructured/load-orc)

</details>

<details>
<summary>Loading Data From Other Databases</summary>

- [How to Sync Full and Incremental MySQL Changes into Databend](https://docs.databend.com/guides/load-data/load-db/debezium)
- [How to Sync Full and Incremental PostgreSQL Changes into Databend](https://docs.databend.com/guides/load-data/load-db/flink-cdc)
- [How to Sync Full and Incremental Oracle Changes into Databend](https://docs.databend.com/guides/load-data/load-db/flink-cdc)

</details>

<details>
<summary>Querying Semi-structured Data</summary>

- [How to query directly on Parquet file](https://docs.databend.com/guides/load-data/transform/querying-parquet)
- [How to query directly on CSV file](https://docs.databend.com/guides/load-data/transform/querying-csv)
- [How to query directly on TSV file](https://docs.databend.com/guides/load-data/transform/querying-tsv)
- [How to query directly on NDJSON file](https://docs.databend.com/guides/load-data/transform/querying-ndjson)
- [How to query directly on ORC file](https://docs.databend.com/guides/load-data/transform/querying-orc)
</details>

<details>
<summary>Visualize Tools with Databend</summary>

- [Deepnote](https://docs.databend.com/guides/visualize/deepnote)
- [Grafana](https://docs.databend.com/guides/visualize/grafana)
- [Jupyter Notebook](https://docs.databend.com/guides/visualize/jupyter)
- [Metabase](https://docs.databend.com/guides/visualize/metabase)
- [MindsDB](https://docs.databend.com/guides/visualize/mindsdb)
- [Redash](https://docs.databend.com/guides/visualize/redash)
- [Superset](https://docs.databend.com/guides/visualize/superset)
- [Tableau](https://docs.databend.com/guides/visualize/tableau)

</details>

<details>
<summary>Managing Users</summary>

- [How to Create a User](https://docs.databend.com/sql/sql-commands/ddl/user/user-create-user)
- [How to Grant Privileges to a User](https://docs.databend.com/sql/sql-commands/ddl/user/grant#granting-privileges)
- [How to Revoke Privileges from a User](https://docs.databend.com/sql/sql-commands/ddl/user/revoke#revoking-privileges)
- [How to Create a Role](https://docs.databend.com/sql/sql-commands/ddl/user/user-create-role)
- [How to Grant Privileges to a Role](https://docs.databend.com/sql/sql-commands/ddl/user/grant#granting-role)
- [How to Grant Role to a User](https://docs.databend.com/sql/sql-commands/ddl/user/grant)
- [How to Revoke the Role of a User](https://docs.databend.com/sql/sql-commands/ddl/user/revoke#revoking-role)
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

- [COPY-INTO](https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table)
- [INSERT](https://docs.databend.com/sql/sql-commands/dml/dml-insert)
- [DELETE](https://docs.databend.com/sql/sql-commands/dml/dml-delete-from)
- [UPDATE](https://docs.databend.com/sql/sql-commands/dml/dml-update)
- [REPLACE](https://docs.databend.com/sql/sql-commands/dml/dml-replace)
- [MERGE-INTO](https://docs.databend.com/sql/sql-commands/dml/dml-merge)
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
<summary>Data Management</summary>

- [Data Lifecycle in Databend](https://docs.databend.com/guides/data-management/data-lifecycle)
- [Data Recovery in Databend](https://docs.databend.com/guides/data-management/data-recovery)
- [Data Protection in Databend](https://docs.databend.com/guides/data-management/data-protection)
- [Data Purge in Databend](https://docs.databend.com/guides/data-management/data-recycle)

</details>

<details>
<summary>Accessing Data Lake</summary>

- [Apache Hive](https://docs.databend.com/guides/access-data-lake/hive)
- [Apache Iceberg](https://docs.databend.com/guides/access-data-lake/iceberg/)
- [Delta Lake](https://docs.databend.com/guides/access-data-lake/delta)

</details>

<details>
<summary>Security</summary>

- [Access Control](https://docs.databend.com/guides/security/access-control)
- [Masking Policy](https://docs.databend.com/guides/security/masking-policy)
- [Network Policy](https://docs.databend.com/guides/security/network-policy)
- [Password Policy](https://docs.databend.com/guides/security/password-policy)

</details>

<details>
<summary>Performance</summary>

- [Review Clickbench](https://databend.com/blog/clickbench-databend-top)
- [TPC-H Benchmark: Databend Cloud vs. Snowflake](https://docs.databend.com/guides/benchmark/tpch)
- [Databend vs. Snowflake: Data Ingestion Benchmark](https://docs.databend.com/guides/benchmark/data-ingest)

</details>

## 🤝 Contributing

Databend thrives on community contributions! Whether it's through ideas, code, or documentation, every effort helps in enhancing our project. As a token of our appreciation, once your code is merged, your name will be eternally preserved in the **system.contributors** table.

Here are some resources to help you get started:

- [Building Databend From Source](https://docs.databend.com/developer/community/contributor/building-from-source)
- [The First Good Pull Request](https://docs.databend.com/developer/community/contributor/good-pr)

## 👥 Community

For guidance on using Databend, we recommend starting with the official documentation. If you need further assistance, explore the following community channels:

- [Slack](https://link.databend.com/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/databendlabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs/) (Get the news fast)
- [I'm feeling lucky](https://link.databend.com/i-m-feeling-lucky) (Pick up a good first issue now!)

## 🛣️ Roadmap

Stay updated with Databend's development journey. Here are our roadmap milestones:

- [Roadmap 2025](https://github.com/databendlabs/databend/issues/14167)

## 📜 License

Databend is released under a combination of two licenses: the [Apache License 2.0](licenses/Apache-2.0.txt) and the [Elastic License 2.0](licenses/Elastic.txt).

When contributing to Databend, you can find the relevant license header in each file.

For more information, see the [LICENSE](LICENSE) file and [Licensing FAQs](https://docs.databend.com/guides/products/dee/license).

## 🙏 Acknowledgement

- **Inspiration**: Databend's design draws inspiration from industry leaders [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture).

- **Computing Model**: Our computing foundation is built upon apache arrow.

- **Documentation Hosting**: The [Databend documentation website](https://docs.databend.com) proudly runs on [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss).
