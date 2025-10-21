# Test protocol compatibility between latest and old databend-meta

Tests cross-version compatibility of databend-meta, ensuring new versions can communicate with old versions in a cluster.

## Prerequisites

- Current version binaries must exist in `./bins/current/bin/`:
    - `databend-meta`
    - `databend-metactl`
    - `databend-metabench`

Since building binaries takes several minutes, this is usually done by CI scripts.

## Usage

```bash
python3 test_meta_meta.py <leader-version> <follower-version>
```

Examples:
```bash
# Test current version with old version
python3 test_meta_meta.py current 1.2.615

# Test two old versions
python3 test_meta_meta.py 1.2.615 1.2.600
```

The script automatically downloads old version binaries from GitHub releases if not present in `./bins/<version>/`.

## Tests

### test_snapshot_replication

Tests snapshot-based replication between versions:
1. Start leader node, feed data
2. Trigger snapshot, feed more data
3. Start follower node (joins via snapshot)
4. Verify state machine data consistency
5. Re-import with current metactl and verify again

### test_vote_request

Tests vote request compatibility between versions:
1. Start two-node cluster with different versions
2. Feed data, restart cluster
3. Feed more data, trigger snapshots
4. Verify state machine data consistency

This test normalizes time precision differences:
- `time_ms`: truncates last 3 digits to 000
- `expire_at`: converts milliseconds to seconds

## Compatibility Handling

The tests account for version differences:
- **proposed_at_ms**: Removed during comparison (not present in all versions)
- **Time precision**: Normalized in vote_request test (ms vs sec)
- **DataHeader**: Excluded from comparison (contains version info)
- **NodeId**: Excluded from re-import comparison
- **Expire/GenericKV**: Excluded during re-import (format changed in V002)