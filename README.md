<h1 align="center">Databend</h1>
<h3 align="center">Unified Multimodal Database for Any Data at Any Scale.</h3>
<p align="center">A <strong>next-generation</strong> cloud-native warehouse built in <strong>Rust</strong>. Open-source, Snowflake-compatible, and unifying BI, AI, Search, Geo, and Stream.</p>

<div align="center">

<a href="https://databend.com/">☁️ Try Cloud</a> •
<a href="#-quick-start">🚀 Quick Start</a> •
<a href="https://docs.databend.com/">📖 Documentation</a> •
<a href="https://link.databend.com/join-slack">💬 Slack</a>

<br><br>

<a href="https://github.com/databendlabs/databend/actions/workflows/release.yml">
<img src="https://img.shields.io/github/actions/workflow/status/datafuselabs/databend/release.yml?branch=main" alt="CI Status" />
</a>
<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Platform" />

</div>

<br>

<img src="https://github.com/user-attachments/assets/4c288d5c-9365-44f7-8cde-b2c7ebe15622" alt="databend" width="100%" />

## 💡 Why Databend?

Databend is an open-source **unified multimodal database** built in Rust. It empowers **Analytics**, **AI**, **Search**, and **Geo** workloads on a single platform directly from object storage.

- **Unified Engine**: One optimizer and runtime for all data types (Structured, Semi-structured, Vector).
- **Native Pipelines**: Built-in **Stream** and **Task** for automated data cleaning and transformation.
- **Cloud Native**: Stateless compute nodes over object storage (S3, GCS, Azure) with full ACID support.
- **High Performance**: Vectorized execution and Zero-Copy processing.

## ⚡ Quick Start

### 1. Cloud (Recommended)
[Start for free on Databend Cloud](https://docs.databend.com/guides/cloud/) - Production-ready in 60 seconds.

### 2. Local (Python)
Ideal for development and testing:

```bash
pip install databend
```

```python
import databend
ctx = databend.SessionContext()
ctx.sql("SELECT 'Hello, Databend!'").show()
```

### 3. Docker
Run the full warehouse locally:

```bash
docker run -p 8000:8000 datafuselabs/databend
```

## 🚀 Use Cases

- **BI & Analytics**: High-speed SQL on massive datasets. See [Query Processing](https://docs.databend.com/guides/query/query-processing).
- **AI & Vectors**: Built-in vector search and embedding management. See [Vector Database](https://docs.databend.com/guides/query/vector-db).
- **Full-Text Search**: Fast indexing and retrieval on text and semi-structured data (JSON). See [Full-Text Index](https://docs.databend.com/guides/query/full-text).
- **Geospatial**: Advanced geo-analytics and mapping. See [Geospatial Analysis](https://docs.databend.com/guides/query/geospatial).
- **Stream & Task**: Continuous data ingestion and transformation. See [Lakehouse ETL](https://docs.databend.com/guides/query/lakehouse-etl).

## 🤝 Community & Support

- [📖 Documentation](https://docs.databend.com/)
- [💬 Join Slack](https://link.databend.com/join-slack)
- [🐛 Issue Tracker](https://github.com/databendlabs/databend/issues)
- [🗺️ Roadmap](https://github.com/databendlabs/databend/issues/14167)

**Contributors are immortalized in the `system.contributors` table! 🏆**

## 📄 License

[Apache 2.0](licenses/Apache-2.0.txt) + [Elastic 2.0](licenses/Elastic.txt) | [Licensing FAQ](https://docs.databend.com/guides/products/dee/license)

---

<div align="center">
<strong>Built by engineers who redefine what's possible with data</strong><br>
<a href="https://databend.com">🌐 Website</a> •
<a href="https://x.com/DatabendLabs">🐦 Twitter</a>
</div>
