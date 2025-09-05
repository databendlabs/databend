# Databend Python Binding

Official Python binding for [Databend](https://databend.com) - A modern cloud data warehouse with vectorized execution and elastic scaling.

## Installation

```bash
pip install databend
```

## Quick Start

```python
import databend

# Initialize service (call once at startup)
databend.init_service(local_dir=".databend")

# Create session
ctx = databend.SessionContext()

# Execute SQL
df = ctx.sql("SELECT number, number + 1 FROM numbers(5)")
df.show()

# Convert to pandas/polars
pandas_df = df.to_pandas()
polars_df = df.to_polars()
```

## Core Features

**SQL Analytics**: Full SQL support with advanced functions
```python
ctx.sql("SELECT department, AVG(salary) FROM employees GROUP BY department").show()
```

**External Data**: Register and query files directly
```python
ctx.register_parquet("events", "/path/to/events/", pattern="*.parquet")  
ctx.register_csv("users", "/path/to/users.csv")
df = ctx.sql("SELECT * FROM events JOIN users ON events.user_id = users.id")
```

**Multi-tenant**: Isolated data per tenant
```python
ctx_a = databend.SessionContext(tenant="team_a")
ctx_b = databend.SessionContext(tenant="team_b")  # Completely separate
```

## Development

Setup virtualenv:

```shell
uv sync
```

Activate venv:

```shell
source .venv/bin/activate
````

Install `maturin`:

```shell
pip install "maturin[patchelf]"
```

Build bindings:

```shell
uvx maturin develop
```

Run tests:

```shell
uvx maturin develop -E test
```

Build API docs:

```shell
uvx maturin develop -E docs
uvx pdoc databend
```

## Service configuration

> Note:

**`databend.init_service`  must be initialized before `SessionContext`**

**`databend.init_service`  must be called only once**


-  By default, you can init the service by a local directory, then data & catalogs will be stored inside the directory.
```
import databend

databend.init_service(local_dir = ".databend")
```

-  You can also init by file

```
import databend
databend.init_service( config = "config.toml.sample" )
```

- And by config str
```
import databend

databend.init_service(config = """
[meta]
embedded_dir = "./.databend/"

# Storage config.
[storage]
# fs | s3 | azblob | obs | oss
type = "fs"
allow_insecure = true

[storage.fs]
data_path = "./.databend/"
""")
```

Read more about configs of databend in [docs](https://docs.databend.com/guides/deploy/deploy/production/metasrv-deploy)

## More
Databend python api is inspired by [arrow-datafusion-python](https://github.com/apache/arrow-datafusion-python), thanks for their great work.
