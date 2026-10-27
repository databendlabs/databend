<h1 align="center">Databend</h1>
<h3 align="center">Enterprise Data Warehouse for AI Agents</h3>
<h4 align="center">one warehouse for all your data</h4>
<p align="center">SQL analytics, vector search, and full-text search in a single Rust warehouse on object storage: fast, elastic, and grows with your data — with sandboxed Python UDFs for agents.</p>

<div align="center">

<a href="https://databend.com/">☁️ Try Cloud</a> •
<a href="#-quick-start">🚀 Quick Start</a> •
<a href="https://docs.databend.com/">📖 Documentation</a> •
<a href="https://link.databend.com/join-slack">💬 Slack</a>

<br><br>

<a href="https://github.com/databendlabs/databend/actions/workflows/release.yml">
<img src="https://img.shields.io/github/actions/workflow/status/databendlabs/databend/release.yml?branch=main" alt="Release Status" />
</a>
<img src="https://img.shields.io/badge/Platform-Linux%2C%20macOS%2C%20ARM-green.svg?style=flat" alt="Platform" />

</div>

<br>

<img src="https://github.com/user-attachments/assets/4c288d5c-9365-44f7-8cde-b2c7ebe15622" alt="databend" width="100%" />

## 💡 Why Databend?

Databend is an open-source data warehouse built in Rust. Analytics, search, and agent workloads run on the same tables, so there is no second system to sync.

| | |
| :--- | :--- |
| **📊 One Warehouse**<br>SQL analytics, vector search, full-text search, auto schema evolution, transactions. | **🤖 Agent-Ready**<br>Sandboxed Python UDFs for agent logic, SQL for orchestration. |
| **🏢 Enterprise Scale**<br>Fast vectorized engine, elastic compute on object storage: S3, Azure, GCS. | **🌿 Branching**<br>Git-like data versioning. Agents experiment safely on production snapshots. |

![Databend Architecture](https://github.com/user-attachments/assets/288dea8d-0243-4c45-8d18-d4d402b08075)

## ⚡ Quick Start

**Cloud (Recommended)** — [Sign up](https://app.databend.com/), create a warehouse, and run SQL in the worksheet. [Cloud guide →](https://docs.databend.com/guides/cloud/)

**Self-hosted** — Run the all-in-one image and connect with [BendSQL](https://docs.databend.com/guides/connect/sql-clients/bendsql):

```bash
docker run -d -p 127.0.0.1:8000:8000 datafuselabs/databend
bendsql -u root
```

See [Docker image options](docker/README.md) and [self-hosted deployment](https://docs.databend.com/guides/self-hosted) for credentials, storage, and production setup.

### Your First Query

Analytics, full-text search, and vector search in one table:

```sql
CREATE TABLE docs (
    id INT,
    content STRING,
    embedding VECTOR(3),
    INVERTED INDEX content_idx(content),
    VECTOR INDEX embedding_idx(embedding) distance = 'cosine'
);

INSERT INTO docs VALUES
    (1, 'SQL analytics for agents', [1.0, 0.0, 0.0]),
    (2, 'Vector search for documents', [0.0, 1.0, 0.0]);

SELECT id, content FROM docs WHERE MATCH(content, 'analytics');

SELECT id, content FROM docs
ORDER BY COSINE_DISTANCE(embedding, [1.0, 0.0, 0.0]::VECTOR(3))
LIMIT 1;
```

Both queries return document `1`. Learn more: [Vector search](https://docs.databend.com/guides/query/vector-db) · [JSON & search](https://docs.databend.com/guides/query/json-search).

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
    answer = f"processed: {input}"
    return answer
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
- [🛠️ Contributing](AI_POLICY.md) — AI-assisted PRs are welcome; a responsible human must review the full diff

**Contributors are immortalized in the `system.contributors` table 🏆**

## 📄 License

Databend is licensed under [Apache 2.0](LICENSE). A small set of enterprise features is under [Elastic 2.0](licenses/Elastic.txt), see [LICENSING.md](LICENSING.md) | [Licensing FAQ](https://docs.databend.com/guides/products/dee/license)

---

<div align="center">
<strong>Enterprise warehouse, agent ready</strong><br>
<a href="https://databend.com">🌐 Website</a> •
<a href="https://x.com/DatabendLabs">🐦 Twitter</a>
</div>
