<h1 align="center">Databend</h1>
<h2 align="center">ANY DATA. ANY SCALE. ONE DATABASE.</h2>
<h3 align="center">Blazing analytics, fast search, geo insights, vector AI — supercharged in a new-era Snowflake-compatible warehouse</h3>

<div align="center">

<a href="https://databend.com/">☁️ Try Cloud</a> •
<a href="#quick-start">🚀 Quick Start</a> •
<a href="https://docs.databend.com/">📖 Documentation</a>

<br><br>

<a href="https://link.databend.com/join-slack">
<img src="https://img.shields.io/badge/slack-databend-0abd59?logo=slack" alt="slack" />
</a>
<a href="https://github.com/databendlabs/databend/actions/workflows/release.yml">
<img src="https://img.shields.io/github/actions/workflow/status/datafuselabs/databend/release.yml?branch=main" alt="CI Status" />
</a>
<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Platform" />

</div>

<br>

<img src="https://github.com/user-attachments/assets/4c288d5c-9365-44f7-8cde-b2c7ebe15622" alt="databend" />

## Why Databend?

Databend has evolved from an analytics warehouse into a unified multimodal database that powers AI, search, and geospatial workloads side by side with classic analytics — all through the same Snowflake-compatible SQL surface.

**Unified Multimodal Engine**: Analytics, vector search, full-text, and geospatial run on one elastic engine with a shared optimizer and execution runtime.

**Rust-Powered & Cost Efficient**: Vectorized Rust execution with separated storage delivers fast performance while keeping compute spend lean.

**Store Everything, Query Anything**: Structured, semi-structured, vector, and unstructured media data live together in columnar storage with indexes, caching, and transactional guarantees.

**AI-Native & Developer Friendly**: Built-in embeddings, semantic ranking, and SDKs for Python, Java, and AI frameworks make it easy to ship intelligent applications.

**Cloud Scale, Edge Ready**: Deploy Databend Cloud, self-host in your environment, or run locally with `pip install databend` — every deployment speaks the same protocols and reads the same open storage.

**Enterprise Grade**: Fine-grained governance, masking, auditing, and proven production scale handling 800+ PB of data and 100M+ daily queries.

## Quick Start

### Option 1: Databend Cloud Warehouse (Recommended)
[Start with Databend Cloud](https://docs.databend.com/guides/cloud/) - Serverless warehouse clusters, production-ready in 60 seconds

### Option 2: Local Development with Python
```bash
pip install databend
```

```python
import databend

ctx = databend.SessionContext()

# Local table for quick testing
ctx.sql("CREATE TABLE products (id INT, name STRING, price FLOAT)").collect()
ctx.sql("INSERT INTO products VALUES (1, 'Laptop', 1299.99), (2, 'Phone', 899.50)").collect()
ctx.sql("SELECT * FROM products").show()

# S3 remote table (same as cloud warehouse)
ctx.create_s3_connection("s3", "your_key", "your_secret")
ctx.sql("CREATE TABLE sales (id INT, revenue FLOAT) 's3://bucket/sales/' CONNECTION=(connection_name='s3')").collect()
ctx.sql("SELECT COUNT(*) FROM sales").show()
```

### Option 3: Docker (Self-Host Experience)
```bash
docker run -p 8000:8000 datafuselabs/databend
```
Experience the full warehouse capabilities locally - same features as cloud clusters.

## Benchmarks

**Performance**: [TPC-H vs Snowflake](https://docs.databend.com/guides/benchmark/tpch) | [ClickBench Results](https://www.databend.com/blog/category-product/clickbench-databend-top)
**Cost**: [90% Cost Reduction](https://docs.databend.com/guides/benchmark/data-ingest)

## Architecture

![Databend Architecture](https://github.com/databendlabs/databend/assets/172204/68b1adc6-0ec1-41d4-9e1d-37b80ce0e5ef)

**Multimodal Cloud Warehouse**: Production clusters analyze structured, semi-structured, vector, and geospatial data with Snowflake-compatible SQL. Local development environments can attach to the same warehouse data for seamless development.

## Use Cases

- **Warehouse Analytics**: ANSI SQL joins, window functions, incremental aggregates, and streaming ingestion for BI workloads.
- **AI & Vector Systems**: Persist embeddings alongside facts, index vectors, and run semantic retrieval for RAG and agent pipelines.
- **Hybrid Search**: Mix full-text search over JSON, metadata filters, and semantic similarity inside one query plan.
- **Geospatial Processing**: Run distance, containment, and hex-grid analytics to power maps and mobility scenarios.
- **Lakehouse ETL**: Query Parquet, CSV, and NDJSON in object storage, transform streaming updates, and load them into managed tables.

## Community

- [📖 Documentation](https://docs.databend.com/) - Complete guides and references
- [💬 Slack](https://link.databend.com/join-slack) - Live community discussion
- [🐛 GitHub Issues](https://github.com/databendlabs/databend/issues) - Bug reports and feature requests
- [🎯 Good First Issues](https://link.databend.com/i-m-feeling-lucky) - Start contributing today

**Contributors get immortalized in `system.contributors` table! 🏆**

## 📄 License

[Apache License 2.0](licenses/Apache-2.0.txt) + [Elastic License 2.0](licenses/Elastic.txt)
[Licensing FAQs](https://docs.databend.com/guides/products/dee/license)

---

<div align="center">
<strong>Built by engineers who redefine what's possible with data</strong><br>
<a href="https://databend.com">🌐 Website</a> •
<a href="https://x.com/DatabendLabs">🐦 Twitter</a> •
<a href="https://github.com/databendlabs/databend/issues/14167">🗺️ Roadmap</a>
</div>
