<h1 align="center">Databend</h1>
<h3 align="center">Enterprise Data Warehouse for AI Agents</h3>
<p align="center">Large-scale analytics, vector search, full-text search — with flexible agent orchestration and secure Python UDF sandboxes. Built for enterprise AI workloads.</p>

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

Databend is an open-source enterprise data warehouse built in Rust.

**Core capabilities**: Analytics, vector search, full-text search, auto schema evolution — unified in one engine.

**Agent-ready**: Sandbox UDFs for agent logic, SQL for orchestration, transactions for reliability, branching for safe experimentation on production data.

| | |
| :--- | :--- |
| **📊 Core Engine**<br>Analytics, vector search, full-text search, auto schema evolution, transactions. | **🤖 Agent-Ready**<br>Sandbox UDF + SQL orchestration. Build and run agents on your enterprise data. |
| **🏢 Enterprise Scale**<br>Elastic compute, cloud native. S3/Azure/GCS. | **🌿 Branching**<br>Git-like data versioning. Agents safely operate on production snapshots. |

![Databend Architecture](https://github.com/user-attachments/assets/288dea8d-0243-4c45-8d18-d4d402b08075)

## ⚡ Quick Start

### 1. Cloud (Recommended)
[Start for free on Databend Cloud](https://docs.databend.com/guides/cloud/) — Production-ready in 60 seconds.

### 2. Local (Python)
Ideal for development and testing. Requires Python 3.12 or 3.13 and `databend-driver` 0.34.0 or later:

```bash
pip install "databend-driver[local]>=0.34.0"
```

```python
from databend_driver import connect

conn = connect("databend+local:///./local-state")
print(conn.query_row("SELECT 'Hello, Databend!'").values())
```

### 3. Docker
Run the full warehouse locally:

```bash
docker run -p 8000:8000 datafuselabs/databend
```

## 🤖 Agent-Ready Architecture

Databend's **Sandbox UDF** enables flexible agent orchestration with a three-layer architecture:

- **Control Plane**: Resource scheduling, permission validation, sandbox lifecycle management
- **Execution Plane** (Databend): SQL orchestration, issues requests via Arrow Flight
- **Compute Plane** (Sandbox Workers): Isolated sandboxes running your agent logic

```sql
-- Define your agent logic
CREATE FUNCTION my_agent(input STRING) RETURNS STRING
LANGUAGE python HANDLER = 'run'
AS $$
def run(input):
    # Your agent logic: LLM calls, tool use, reasoning...
    return response
$$;

-- Orchestrate agents with SQL
SELECT my_agent(question) FROM tasks;
```

## 🚀 Use Cases

- **AI Agents**: Sandbox UDF + SQL orchestration + branching for safe operations
- **Analytics & BI**: Large-scale SQL analytics — [Learn more](https://docs.databend.com/guides/query/sql-analytics)
- **Search & RAG**: Vector + full-text search — [Learn more](https://docs.databend.com/guides/query/vector-db)

## 🤝 Community & Support

- [📖 Documentation](https://docs.databend.com/)
- [💬 Join Slack](https://link.databend.com/join-slack)
- [🐛 Issue Tracker](https://github.com/databendlabs/databend/issues)
- [🗺️ Roadmap](https://github.com/databendlabs/databend/issues/14167)

**Contributors are immortalized in the `system.contributors` table 🏆**

## 📄 License

[Apache 2.0](licenses/Apache-2.0.txt) + [Elastic 2.0](licenses/Elastic.txt) | [Licensing FAQ](https://docs.databend.com/guides/products/dee/license)

---

<div align="center">
<strong>Enterprise warehouse, agent ready</strong><br>
<a href="https://databend.com">🌐 Website</a> •
<a href="https://x.com/DatabendLabs">🐦 Twitter</a>
</div>
