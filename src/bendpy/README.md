# Databend local mode Python Binding

Python binding for [Databend](https://databend.com) in local mode - The multi-modal data warehouse built for the AI era.

Databend unifies structured data, JSON documents, and vector embeddings in a single platform with near 100% Snowflake compatibility. Built in Rust with MPP architecture and S3-native storage for cloud-scale analytics.

## Installation

```bash
pip install databend
```

To test, run:
```python
python3 -c "import databend; ctx = databend.SessionContext(); ctx.sql('SELECT version() AS version').show()"
```

## API Reference

### Core Operations
| Method | Description |
|--------|-------------|
| `connect(path=":memory:")` | Create a local embedded connection |
| `SessionContext()` | Create a new session context |
| `sql(query)` | Execute SQL query, returns DataFrame |
| `execute(query)` | Execute SQL and return the connection |
| `table(name)` | Query a registered table or view |
| `register(name, source)` | Register a local path, pandas/polars frame, or Arrow table |
| `from_df(obj, name=None)` | Materialize an in-process dataframe-like object as a relation |

### File Registration
| Method                                                        | Description |
|---------------------------------------------------------------|-------------|
| `register_parquet(name, path, pattern=None, connection=None)` | Register Parquet files as table |
| `register_csv(name, path, pattern=None, connection=None)`     | Register CSV files as table |
| `register_ndjson(name, path, pattern=None, connection=None)`  | Register NDJSON files as table |
| `register_text(name, path, pattern=None, connection=None)`    | Register TEXT files as table |

### Cloud Storage Connections
| Method | Description |
|--------|-------------|
| `create_s3_connection(name, key, secret, endpoint=None, region=None)` | Create S3 connection |
| `create_azblob_connection(name, url, account, key)` | Create Azure Blob connection |
| `create_gcs_connection(name, url, credential)` | Create Google Cloud connection |
| `list_connections()` | List all connections |
| `describe_connection(name)` | Show connection details |
| `drop_connection(name)` | Remove connection |

### Stage Management
| Method | Description |
|--------|-------------|
| `create_stage(name, url, connection)` | Create external stage |
| `show_stages()` | List all stages |
| `list_stages(stage_name)` | List files in stage |
| `describe_stage(name)` | Show stage details |
| `drop_stage(name)` | Remove stage |

### DataFrame Operations
| Method | Description |
|--------|-------------|
| `collect()` | Execute and collect results |
| `show(num=20)` | Display results in console |
| `to_pandas()` | Convert to pandas DataFrame |
| `to_polars()` | Convert to polars DataFrame |
| `to_arrow_table()` | Convert to PyArrow Table |
| `df()` | DuckDB-style alias for `to_pandas()` |
| `pl()` | Alias for `to_polars()` |
| `arrow()` | Alias for `to_arrow_table()` |
| `fetchall()` | Collect result rows as Python tuples |
| `fetchone()` | Return the first row or `None` |

## Examples

### Local Tables

```python
import databend
ctx = databend.connect()

# Create and query in-memory tables
ctx.execute("CREATE TABLE users (id INT, name STRING, age INT)")
ctx.execute("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
df = ctx.sql("SELECT * FROM users WHERE age > 25").df()
```

### Working with Local Files

```python
import databend
ctx = databend.connect("./demo-data")

# Query local Parquet files
ctx.read_parquet("/path/to/orders/", name="orders")
ctx.register("customers", "/path/to/customers.parquet")
df = ctx.sql("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id").df()
```

### Working with In-Process DataFrames

```python
import databend
import pandas as pd

ctx = databend.connect("./memory-store")
docs = pd.DataFrame(
    {
        "id": [1, 2],
        "content": ["hello", "vector memory"],
    }
)

ctx.register("docs", docs)
rows = ctx.sql("SELECT id, content FROM docs ORDER BY id").fetchall()
```

### Cloud Storage - S3 Files

```python
import databend
import os
ctx = databend.SessionContext()

# Connect to S3 and query remote files
ctx.create_s3_connection("s3", os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
ctx.register_parquet("trips", "s3://bucket/trips/", connection="s3")
df = ctx.sql("SELECT COUNT(*) FROM trips").to_pandas()
```

### Cloud Storage - S3 Tables

```python
import databend
import os
ctx = databend.SessionContext()

# Create S3 connection and persistent table
ctx.create_s3_connection("s3", os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
ctx.sql("CREATE TABLE s3_table (id INT, name STRING) 's3://bucket/table/' CONNECTION=(CONNECTION_NAME='s3')").collect()
df = ctx.sql("SELECT * FROM s3_table").to_pandas()
```


## Development

```bash
# Setup environment
uv sync
source .venv/bin/activate

# Run tests
uvx maturin develop -E test
pytest tests/
```
