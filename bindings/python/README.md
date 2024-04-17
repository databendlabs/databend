# databend-driver

## Build

```shell
cd bindings/python
maturin develop
```

## Usage

### Blocking

```python
from databend_driver import BlockingDatabendClient

client = BlockingDatabendClient('databend+http://root:root@localhost:8000/?sslmode=disable')
conn = client.get_conn()
conn.exec(
    """
    CREATE TABLE test (
        i64 Int64,
        u64 UInt64,
        f64 Float64,
        s   String,
        s2  String,
        d   Date,
        t   DateTime
    )
    """
)
rows = conn.query_iter("SELECT * FROM test")
for row in rows:
    print(row.values())
```

### Asyncio

```python
import asyncio
from databend_driver import AsyncDatabendClient

async def main():
    client = AsyncDatabendClient('databend+http://root:root@localhost:8000/?sslmode=disable')
    conn = await client.get_conn()
    await conn.exec(
        """
        CREATE TABLE test (
            i64 Int64,
            u64 UInt64,
            f64 Float64,
            s   String,
            s2  String,
            d   Date,
            t   DateTime
        )
        """
    )
    rows = await conn.query_iter("SELECT * FROM test")
    async for row in rows:
        print(row.values())

asyncio.run(main())
```

## Type Mapping

[Databend Types](https://docs.databend.com/sql/sql-reference/data-types/)

### General Data Types

| Databend    | Python              |
| ----------- | ------------------- |
| `BOOLEAN`   | `bool`              |
| `TINYINT`   | `int`               |
| `SMALLINT`  | `int`               |
| `INT`       | `int`               |
| `BIGINT`    | `int`               |
| `FLOAT`     | `float`             |
| `DOUBLE`    | `float`             |
| `DECIMAL`   | `decimal.Decimal`   |
| `DATE`      | `datetime.date`     |
| `TIMESTAMP` | `datetime.datetime` |
| `VARCHAR`   | `str`               |
| `BINARY`    | `bytes`             |

### Semi-Structured Data Types

| Databend   | Python  |
| ---------- | ------- |
| `ARRAY`    | `list`  |
| `TUPLE`    | `tuple` |
| `MAP`      | `dict`  |
| `VARIANT`  | `str`   |
| `BITMAP`   | `str`   |
| `GEOMETRY` | `str`   |

## APIs

### AsyncDatabendClient

```python
class AsyncDatabendClient:
    def __init__(self, dsn: str): ...
    async def get_conn(self) -> AsyncDatabendConnection: ...
```

### AsyncDatabendConnection

```python
class AsyncDatabendConnection:
    async def info(self) -> ConnectionInfo: ...
    async def version(self) -> str: ...
    async def exec(self, sql: str) -> int: ...
    async def query_row(self, sql: str) -> Row: ...
    async def query_iter(self, sql: str) -> RowIterator: ...
    async def stream_load(self, sql: str, data: list[list[str]]) -> ServerStats: ...
```

### BlockingDatabendClient

```python
class BlockingDatabendClient:
    def __init__(self, dsn: str): ...
    def get_conn(self) -> BlockingDatabendConnection: ...
```

### BlockingDatabendConnection

```python
class BlockingDatabendConnection:
    def info(self) -> ConnectionInfo: ...
    def version(self) -> str: ...
    def exec(self, sql: str) -> int: ...
    def query_row(self, sql: str) -> Row: ...
    def query_iter(self, sql: str) -> RowIterator: ...
    def stream_load(self, sql: str, data: list[list[str]]) -> ServerStats: ...
```

### Row

```python
class Row:
    def values(self) -> tuple: ...
```

### RowIterator

```python
class RowIterator:
    def schema(self) -> Schema: ...

    def __iter__(self) -> RowIterator: ...
    def __next__(self) -> Row: ...

    def __aiter__(self) -> RowIterator: ...
    async def __anext__(self) -> Row: ...
```

### Field

```python
class Field:
    @property
    def name(self) -> str: ...
    @property
    def data_type(self) -> str: ...
```

### Schema

```python
class Schema:
    def fields(self) -> list[Field]: ...
```

### ServerStats

```python
class ServerStats:
    @property
    def total_rows(self) -> int: ...
    @property
    def total_bytes(self) -> int: ...
    @property
    def read_rows(self) -> int: ...
    @property
    def read_bytes(self) -> int: ...
    @property
    def write_rows(self) -> int: ...
    @property
    def write_bytes(self) -> int: ...
    @property
    def running_time_ms(self) -> float: ...
```

### ConnectionInfo

```python
class ConnectionInfo:
    @property
    def handler(self) -> str: ...
    @property
    def host(self) -> str: ...
    @property
    def port(self) -> int: ...
    @property
    def user(self) -> str: ...
    @property
    def database(self) -> str | None: ...
    @property
    def warehouse(self) -> str | None: ...
```

## Development

```
cd tests
make up
```

```shell
cd bindings/python
pipenv install --dev
pipenv run maturin develop
pipenv run behave tests/*
```
