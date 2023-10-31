# databend-driver

## Build

```shell
cd bindings/python
maturin develop
```

## Usage

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

## Development

```shell
pipenv install --dev
maturin develop
pipenv run behave tests
```
