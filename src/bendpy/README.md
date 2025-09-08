# Databend Python Binding

Official Python binding for [Databend](https://databend.com) - The AI-Native Data Warehouse.

Databend is the open-source alternative to Snowflake with near 100% SQL compatibility and native AI capabilities. Built in Rust with MPP architecture and S3-native storage, Databend unifies structured tables, JSON documents, and vector embeddings in a single platform.

## Installation

```bash
pip install databend
```

## Quick Start

```python
import databend

# Create session (automatically initializes embedded mode)
ctx = databend.SessionContext()

# Execute SQL
df = ctx.sql("SELECT number, number + 1 FROM numbers(5)")
df.show()

# Convert to pandas/polars
pandas_df = df.to_pandas()
polars_df = df.to_polars()
```

## Examples

### 1. Register External Files and Query

```python
import databend

ctx = databend.SessionContext()

# Register external files
ctx.register_parquet("pa", "/home/dataset/hits_p/", pattern=".*.parquet")
ctx.register_csv("users", "/path/to/users.csv")
ctx.register_ndjson("logs", "/path/to/logs/", pattern=".*.jsonl")

# Query external data
result = ctx.sql("SELECT * FROM pa LIMIT 10").collect()
print(result)
```

### 2. Create Table, Insert and Select

```python
import databend

ctx = databend.SessionContext()

# Create table
ctx.sql("CREATE TABLE aa (a INT, b STRING, c BOOL, d DOUBLE)").collect()

# Insert data
ctx.sql("INSERT INTO aa SELECT number, number, true, number FROM numbers(10)").collect()
ctx.sql("INSERT INTO aa SELECT number, number, true, number FROM numbers(10)").collect()

# Query and convert to pandas
df = ctx.sql("SELECT sum(a) x, max(b) y, max(d) z FROM aa WHERE c").to_pandas()
print(df.values.tolist())  # [[90.0, "9", 9.0]]

# Query and convert to polars  
df_polars = ctx.sql("SELECT sum(a) x, max(b) y, max(d) z FROM aa WHERE c").to_polars()
print(df_polars.to_pandas().values.tolist())  # [[90.0, "9", 9.0]]
```

## API Reference

| Method                                           | Description                             | Example                                        |
|--------------------------------------------------|-----------------------------------------|------------------------------------------------|
| `SessionContext(tenant=None, data_path=".databend")` | Create session context (auto-initializes) | `ctx = databend.SessionContext()`              |
| `ctx.sql(sql)`                                   | Execute SQL and return DataFrame        | `df = ctx.sql("SELECT * FROM table")`          |
| `df.show(num=20)`                                | Display DataFrame results               | `df.show()`                                    |
| `df.collect()`                                   | Collect DataFrame as DataBlocks         | `blocks = df.collect()`                        |
| `df.to_pandas()`                                 | Convert to Pandas DataFrame             | `pdf = df.to_pandas()`                         |
| `df.to_polars()`                                 | Convert to Polars DataFrame             | `pldf = df.to_polars()`                        |
| `df.to_py_arrow()`                               | Convert to PyArrow batches              | `batches = df.to_py_arrow()`                   |
| `df.to_arrow_table()`                            | Convert to PyArrow Table                | `table = df.to_arrow_table()`                  |
| `ctx.register_parquet(name, path, pattern=None)` | Register Parquet files                  | `ctx.register_parquet("data", "/path/")`       |
| `ctx.register_csv(name, path, pattern=None)`     | Register CSV files                      | `ctx.register_csv("users", "/users.csv")`      |
| `ctx.register_ndjson(name, path, pattern=None)`  | Register NDJSON files                   | `ctx.register_ndjson("logs", "/logs/")`        |
| `ctx.register_tsv(name, path, pattern=None)`     | Register TSV files                      | `ctx.register_tsv("data", "/data.tsv")`        |

## Development

```bash
# Setup environment
uv sync
source .venv/bin/activate

# Run tests
uvx maturin develop -E test
pytest tests/
```

