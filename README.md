<h1 align="center">Databend: The Next-Gen Cloud [Data+AI] Analytics</h1>
<h2 align="center">One SQL for All Data: structured, semi-structured & unstructured multimodal data</h2>

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

## The AI-Native Data Warehouse

Databend is the **open-source alternative to Snowflake** with **near 100% SQL compatibility** and native AI capabilities. Built in Rust with MPP architecture and S3-native storage, Databend unifies structured tables, JSON documents, and vector embeddings in a single platform. Trusted by **world-class enterprises** managing **800+ petabytes** and **100+ million queries daily**.

## Key Features

**Performance & Scale**
- **10x Faster**: Rust-powered vectorized execution with SIMD optimization
- **90% Cost Reduction**: S3-native storage eliminates proprietary overhead
- **Infinite Scale**: True compute-storage separation with elastic scaling
- **Production-Proven**: Powers financial analytics, ML pipelines, and real-time AI inference

**Enterprise Ready**
- **Snowflake Compatible**: Migrate with zero SQL rewrites
- **Multi-Cloud**: Deploy on AWS, Azure, GCP, or on-premise
- **Security**: Role-based access, data masking, audit logging
- **No Vendor Lock-in**: Complete data sovereignty and control

## Performance Benchmarks

[TPC-H Benchmark: Databend vs. Snowflake](https://docs.databend.com/guides/benchmark/tpch) | [Data Ingestion Benchmark](https://docs.databend.com/guides/benchmark/data-ingest) | [ClickBench Results](https://databend.com/blog/clickbench-databend-top)

## Architecture

![Databend Architecture](https://github.com/databendlabs/databend/assets/172204/68b1adc6-0ec1-41d4-9e1d-37b80ce0e5ef)

**Unified Foundation**: S3-native storage + MPP query engine + elastic compute clusters

### Universal Data Processing by Type
- **Structured**: Standard SQL with vectorized execution, ACID transactions, enterprise security, and BI integration
- **Semi-Structured**: [VARIANT data type](https://docs.databend.com/sql/sql-reference/data-types/variant) with [virtual columns](https://docs.databend.com/guides/performance/virtual-column) for zero-config automatic JSON acceleration
- **Unstructured**: [Vector data type](https://docs.databend.com/sql/sql-reference/data-types/vector) with HNSW indexing, [AI functions](https://docs.databend.com/sql/sql-functions/ai-functions/), and [full-text search](https://docs.databend.com/guides/performance/fulltext-index) for multimodal workloads

## Quick Start

### Cloud
[Start with Databend Cloud](https://docs.databend.com/guides/cloud/) - Production-ready in 60 seconds

### Self-Hosted
[Installation Guide](https://docs.databend.com/guides/deploy/QuickStart/) - Deploy anywhere with full control

### Connect
[BendSQL CLI](https://docs.databend.com/guides/sql-clients/bendsql) | [Developers Guide](https://docs.databend.com/guides/sql-clients/developers/) 

## Products

- **Open Source**: 100% open source, complete data sovereignty
- **[Databend Cloud](https://databend.com)**: Managed service with serverless autoscaling
- **Enterprise**: Advanced governance, compliance, and support

## Community

For guidance on using Databend, we recommend starting with the official documentation. If you need further assistance, explore the following community channels:

- [Slack](https://link.databend.com/join-slack) (For live discussion with the Community)
- [GitHub](https://github.com/databendlabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs/) (Get the news fast)
- [I'm feeling lucky](https://link.databend.com/i-m-feeling-lucky) (Pick up a good first issue now!)

**Your merged code gets you into the `system.contributors` table. Forever.**

## Roadmap & License

- **Roadmap**: [2025 Development Plan](https://github.com/databendlabs/databend/issues/14167)
- **License**: [Apache License 2.0](licenses/Apache-2.0.txt) + [Elastic License 2.0](licenses/Elastic.txt) | [Licensing FAQs](https://docs.databend.com/guides/products/dee/license)

## Acknowledgement

**Inspiration**: [ClickHouse](https://github.com/clickhouse/clickhouse) and [Snowflake](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html#snowflake-architecture) | **Foundation**: Apache Arrow | **Hosting**: [Vercel](https://vercel.com/?utm_source=databend&utm_campaign=oss)

---

*Built by engineers who redefine what's possible with data.*
