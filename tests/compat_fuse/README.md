# Fuse table compatability test

This script tests whether a newer version databend-query can read fuse table data written
by a older version databend-query.

## Usage

```shell
tests/compat_fuse/test_compat_fuse.sh <old_ver>
tests/compat_fuse/test_compat_fuse_forward.sh <old_ver>
```

E.g. `tests/compat_fuse/test-compat_fuse.sh 0.7.151` tests if the fuse-table written
by **databend-query-0.7.151** can be read by **current** version databend-query.

`tests/compat_fuse/test-fuse-forward-compat.sh 1.2.307` tests if the fuse-table written
by **current** can be read by **databend-query-0.7.151** version databend-query.

## Prerequisites

- Current version of databend-query and databend-meta must reside in `./bins`:
    - `./bins/current/databend-query`
    - `./bins/current/databend-meta`

    Since building a binary takes several minutes,
    this step is usually done by the calling process, e.g., the CI script.


## Testing data

- Suite `tests/compat_fuse/compat-logictest/fuse_compat_write` writes data into a fuse table via an old version query.
- Suite `tests/compat_fuse/compat-logictest/fuse_compat_read` reads the data via current version query.

Fuse table maintainers update these two `logictest` scripts to let the write/read
operations cover fuse-table features.
