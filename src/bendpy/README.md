# Databend Python Binding

Official Python binding for [Databend](https://databend.com) - The AI-Native Data Warehouse.

Databend is the open-source alternative to Snowflake with near 100% SQL compatibility and native AI capabilities. Built in Rust with MPP architecture and S3-native storage, Databend unifies structured tables, JSON documents, and vector embeddings in a single platform.

## Installation

```bash
pip install databend
```

## Examples

### Local Files

```python
import databend
ctx = databend.SessionContext()

# Query local Parquet files
ctx.register_parquet("orders", "/path/to/orders/")
ctx.register_parquet("customers", "/path/to/customers/")
df = ctx.sql("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id").to_pandas()
```

### Local Tables

```python
import databend
ctx = databend.SessionContext()

# Create and query local tables
ctx.sql("CREATE TABLE users (id INT, name STRING, age INT)").collect()
ctx.sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)").collect()
df = ctx.sql("SELECT * FROM users WHERE age > 25").to_pandas()
```

### S3 Remote Files

```python
import databend
import os
ctx = databend.SessionContext()

# Connect to S3 and query remote files
ctx.create_s3_connection("s3", os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
ctx.register_parquet("trips", "s3://bucket/trips/", connection="s3")
df = ctx.sql("SELECT COUNT(*) FROM trips").to_pandas()
```

### Remote Tables

```python
import databend
import os
ctx = databend.SessionContext()

# Create S3 connection and table
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

