# Databend Python Binding

This crate intends to build a native python binding.

## Installation

```bash
pip install databend
```

## Usage

```python
from databend import SessionContext

ctx = SessionContext()

df = ctx.sql("select number, number + 1, number::String as number_p_1 from numbers(8)")

# convert to pyarrow
df.to_py_arrow()

# convert to pandas
df.to_pandas()
```

## Development

Setup virtualenv:

```shell
python -m venv .venv
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
maturin develop
```


## More

Databend python api is inspired by [arrow-datafusion-python](https://github.com/apache/arrow-datafusion-python), thanks for their great work.