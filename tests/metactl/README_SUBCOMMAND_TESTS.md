# Metactl Subcommand Tests

Comprehensive tests for all `databend-metactl` subcommands.

## Usage

### Build Prerequisites
```bash
cargo build --bin databend-metactl --bin databend-meta
```

### Run Tests
```bash
# All tests
python tests/metactl/test_all_subcommands.py

# Individual tests
python tests/metactl/subcommands/<name>.py
```

## Test Structure

```
tests/metactl/
├── test_all_subcommands.py          # Main test runner
├── subcommands/                     # Subcommand test modules
│   ├── __init__.py
│   └── <name>.py                    # Individual subcommand tests
├── metactl_utils.py                 # Metactl utilities
├── utils.py                         # General utilities
├── config/                          # Node configurations
└── data files                       # Test data
```

## Test Execution Order

1. Basic: `status`, `upsert`, `get`
2. Data operations: `watch`, `trigger-snapshot`
3. Export/Import: `export-from-grpc`, `export-from-raft-dir`, `import`
4. Advanced: `transfer-leader`

## CI Integration

```yaml
test-metactl-subcommands:
  script:
    - cargo build --bin databend-metactl --bin databend-meta
    - python tests/metactl/test_all_subcommands.py
```

## Guidelines

- Use global imports: `from utils import`
- Cleanup only on test success
- Check actual results, not just command success
- Follow naming: `subcommands/<name>.py`