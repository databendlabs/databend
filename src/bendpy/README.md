# Databend Python Binding

This crate intends to build a native python binding.

## Installation

```bash
pip install databend
```

## Usage

### Basic:
```python
import databend
databend.init_service(local_dir = ".databend")
# or use config
# databend.init_service( config = "config.toml.sample" )

from databend import SessionContext
ctx = SessionContext()

df = ctx.sql("select number, number + 1, number::String as number_p_1 from numbers(8)")

df.show()
# convert to pyarrow
import pyarrow
df.to_py_arrow()

# convert to pandas
import pandas
df.to_pandas()
```

### Register external table:

***supported functions:***
- register_parquet
- register_ndjson
- register_csv
- register_tsv

```python

ctx.register_parquet("pa", "/home/sundy/dataset/hits_p/", pattern = ".*.parquet")
ctx.sql("select * from pa limit 10").collect()
```

### Tenant separation:

Tenant has it's own catalog and tables

```python
ctx = SessionContext(tenant = "your_tenant_name")
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
