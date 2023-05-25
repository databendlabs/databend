## databend-driver

### Build

```shell
cd bindings/python
maturin develop
```

## Usage

```python
import databend_driver
import asyncio
async def main():
	s = databend_driver.AsyncDatabendDriver('databend+http://root:root@localhost:8000/?sslmode=disable')
	await s.exec("CREATE TABLE if not exists test_upload (x Int32,y VARCHAR)")

asyncio.run(main())
```

## Development

Setup virtualenv:

```shell
python -m venv venv
```

Activate venv:

```shell
source venv/bin/activate
````

Install `maturin`:

```shell
pip install maturin[patchelf]
```

Build bindings:

```shell
maturin develop
```

Run some tests:

```shell
maturin develop -E test
behave tests
```