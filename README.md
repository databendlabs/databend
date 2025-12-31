<h1 align="center">Databend</h1>
<h3 align="center">One Rust Warehouse for Analytics, Search, AI</h3>
<p align="center">Snowflake + Elasticsearch + Vector Search — unified in one Rust-powered warehouse. Native on S3.</p>

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

Databend is an open-source, **All-in-One multimodal database** built in Rust. It seamlessly unifies **Analytics**, **AI**, **Search**, and **Geo** workloads into a single platform, enabling high-performance processing directly on top of object storage.

| | |
| :--- | :--- |
| **📊 BI & Analytics**<br>Supercharge your analytics with a high-performance, vectorized SQL query engine. | **✨ Vector Search**<br>Power AI and RAG applications with built-in, high-speed vector similarity search. |
| **📄 JSON Search**<br>Seamlessly query and analyze semi-structured data with powerful JSON optimization. | **🌍 Geo Search**<br>Efficiently store, index, and query geospatial data for location intelligence. |
| **🔄 ETL Pipeline**<br>Streamline data ingestion and transformation with built-in Streams and Tasks. | **🌿 Branching**<br>Create isolated Copy-on-Write branches instantly for dev, test, or experiments. |

![Databend Architecture](https://github.com/user-attachments/assets/288dea8d-0243-4c45-8d18-d4d402b08075)

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

- **BI & Analytics**: High-speed SQL on massive datasets. See [Query Processing](https://docs.databend.com/guides/query/sql-analytics).
- **AI & Vectors**: Built-in vector search and embedding management. See [Vector Database](https://docs.databend.com/guides/query/vector-db).
- **Full-Text Search**: Fast indexing and retrieval on text and semi-structured data (JSON). See [JSON Search](https://docs.databend.com/guides/query/json-search).
- **Geospatial**: Advanced geo-analytics and mapping. See [Geospatial Analysis](https://docs.databend.com/guides/query/geo-analytics).
- **Stream & Task**: Continuous data ingestion and transformation. See [Real-Time ETL](https://docs.databend.com/guides/query/lakehouse-etl).

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
<strong>Redefining what's possible with data</strong><br>
<a href="https://databend.com">🌐 Website</a> •
<a href="https://x.com/DatabendLabs">🐦 Twitter</a>
</div>
