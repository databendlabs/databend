# Databend Test Helper

Python library for managing Databend processes during testing.

## Installation

```bash
pip install -e .
```

## Usage

```python
from databend_test_helper import DatabendMeta, DatabendQuery

# Start meta service (uses default config)
meta = DatabendMeta()
meta.start()

# Start query service (uses default config)
query = DatabendQuery()
query.start()

# Or use custom config
meta = DatabendMeta(config_path="custom-meta.toml")
query = DatabendQuery(config_path="custom-query.toml")

# Stop services
query.stop()
meta.stop()
```

## Configuration

Default configs are included:
- `databend_test_helper/configs/databend-meta.toml`
- `databend_test_helper/configs/databend-query.toml`

The library automatically parses config files to determine ports for health checking.