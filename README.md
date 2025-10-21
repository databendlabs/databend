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

Databend has expanded from analytics into a unified multimodal database: **one Snowflake-compatible SQL surface for BI, AI, search, and geospatial workloads.**

**Unified Engine**: Analytics, vector, full-text, and geospatial share the same optimizer and elastic runtime.

**Unified Data**: Structured, semi-structured, vector, and unstructured live directly on object stores with indexes, caching, transactions, MVCC branching.

**Analytics Native**: ANSI SQL, windowing, incremental aggregates, and streaming ingestion deliver BI without moving data.

**Vector Native**: Built-in embeddings, vector indexes, and semantic retrieval exposed through SQL and SDKs.

**Search Native**: JSON full-text indexing, structured filters, and ranking to power hybrid search experiences.

**Unified Deployment**: Cloud, self-hosted, or `pip install databend` all run the same engine on shared object storage.

**Rust Performance**: Vectorized Rust execution with separated storage keeps performance high and compute spend lean.

**Enterprise Scale**: Fine-grained governance, masking, auditing, and production deployments exceeding 800+ PB and 100M+ daily queries.

## Benchmarks

**Performance**: [TPC-H vs Snowflake](https://docs.databend.com/guides/benchmark/tpch) | [ClickBench Results](https://www.databend.com/blog/category-product/clickbench-databend-top)
**Cost**: [90% Cost Reduction](https://docs.databend.com/guides/benchmark/data-ingest)


![Databend Architecture](https://github.com/user-attachments/assets/288dea8d-0243-4c45-8d18-d4d402b08075)


## Use Cases

- **SQL Analytics**: ANSI joins, window functions, incremental aggregates, and streaming ingestion for BI workloads.
- **AI Vector**: Persist embeddings alongside facts, index vectors, and run semantic retrieval for RAG and agent pipelines.
- **JSON Search**: Mix full-text search over JSON, metadata filters, and semantic similarity inside one query plan.
- **Geo Analytics**: Run distance, containment, and hex-grid analytics to power maps and mobility scenarios.
- **Lakehouse ETL**: Query Parquet, CSV, and NDJSON in object storage, transform streaming updates, and load them into managed tables.

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
