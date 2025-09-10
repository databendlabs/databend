# Databend Python Binding

Official Python binding for [Databend](https://databend.com) - The multi-modal data warehouse built for the AI era.

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
| `SessionContext()` | Create a new session context |
| `sql(query)` | Execute SQL query, returns DataFrame |

### File Registration
| Method | Description |
|--------|-------------|
| `register_parquet(name, path, pattern=None, connection=None)` | Register Parquet files as table |
| `register_csv(name, path, pattern=None, connection=None)` | Register CSV files as table |
| `register_ndjson(name, path, pattern=None, connection=None)` | Register NDJSON files as table |
| `register_tsv(name, path, pattern=None, connection=None)` | Register TSV files as table |

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

## Examples

### Local Tables

```python
import databend
ctx = databend.SessionContext()

# Create and query in-memory tables
ctx.sql("CREATE TABLE users (id INT, name STRING, age INT)").collect()
ctx.sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").collect()
df = ctx.sql("SELECT * FROM users WHERE age > 25").to_pandas()
```

### Working with Local Files

```python
import databend
ctx = databend.SessionContext()

# Query local Parquet files
ctx.register_parquet("orders", "/path/to/orders/")
ctx.register_parquet("customers", "/path/to/customers/")
df = ctx.sql("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id").to_pandas()
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

