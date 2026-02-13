# Fuse table compatibility test

Tests write-then-read compatibility of fuse-table format across databend-query versions.

- **Backward compat**: old writer → current reader
- **Forward compat**: current writer → old reader

## Test matrix

All test cases are defined in `test_cases.yaml`. Each entry specifies:

| Field      | Description                                                        |
|------------|--------------------------------------------------------------------|
| `writer`   | Query version that writes data (`"current"` = latest build)       |
| `reader`   | Query version that reads data (`"current"` = latest build)        |
| `meta`     | Meta-service versions to run in order (upgrades on-disk meta data) |
| `suite`    | sqllogictest sub-directory under `compat-logictest/`               |

To add a test case, append an entry to `test_cases.yaml`.

## Usage

Run all cases (CI mode):

```shell
python3 tests/compat_fuse/test_compat_fuse.py --run-all
```

Dry run (print cases without executing):

```shell
python3 tests/compat_fuse/test_compat_fuse.py --run-all --dry-run
```

Run a single case (local debugging):

```shell
python3 tests/compat_fuse/test_compat_fuse.py \
    --writer-version 1.2.46 \
    --reader-version current \
    --meta-versions 1.2.527 1.2.677 1.2.833 \
    --logictest-path base
```

## Prerequisites

Current version binaries must reside in `./bins/current/bin/`:

- `databend-query`
- `databend-meta`
- `databend-sqllogictests`

This is handled by the CI setup action. Old version binaries are downloaded automatically.

## Test flow

For each case, the script runs three phases:

1. **Write** — start meta (first version) + query (writer version), run `fuse_compat_write.test`
2. **Meta upgrade** — cycle through all meta versions to upgrade on-disk data
3. **Read** — start meta (last version) + query (reader version), run `fuse_compat_read.test`

## Testing data

Each suite under `compat-logictest/` contains two sqllogictest files:

- `fuse_compat_write.test` — creates tables and writes data
- `fuse_compat_read.test` — reads and verifies the data
