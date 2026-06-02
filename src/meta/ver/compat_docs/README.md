# compat_docs

Compatible version ranges between databend-query and databend-meta-service, and
between databend-meta-service raft nodes.

## For Users

Pre-calculated compatible version ranges:

- [query_to_meta.txt](query_to_meta.txt) — compatible metasrv versions for each query version.
- [meta_to_query.txt](meta_to_query.txt) — compatible query versions for each metasrv version.
- [meta_to_meta.txt](meta_to_meta.txt) — compatible peer metasrv versions for each metasrv version (raft inter-node protocol).

To look up a specific version:

```bash
python3 scripts/find_compat.py -q 1.2.600   # compatible metasrv for query 1.2.600
python3 scripts/find_compat.py -m 1.2.400   # compatible query for metasrv 1.2.400
```

## For Developers

Regenerate: update the known versions in `src/`, then:

```bash
make
```

`make` regenerates `meta_to_meta.txt` from `src/raft-peer-compatibles.txt`, a
snapshot of the databend-meta `raft-compat` binary output (raft compatibility is
defined there, not in this repo's git-tag pipeline). Refresh the snapshot from a
databend-meta checkout when raft features change, then run `make`:

```bash
cargo run -p databend-meta-version --bin raft-compat > <compat_docs>/src/raft-peer-compatibles.txt
```

### Scripts

| File | Purpose |
|---|---|
| [scripts/extract_from_git.py](scripts/extract_from_git.py) | One-time. Extracts version constraints from git tags into [src/min_compatible_versions.txt](src/min_compatible_versions.txt). |
| [scripts/resolve_versions.py](scripts/resolve_versions.py) | Resolves CalVer (e.g. `260205.0.0`) to repo tag versions. Writes [generated/resolved_min_compatibles.txt](generated/resolved_min_compatibles.txt). |
| [scripts/find_compat.py](scripts/find_compat.py) | Prints compatible version range for a given query or metasrv version. |
| [scripts/format_raft_compat.py](scripts/format_raft_compat.py) | Reformats [src/raft-peer-compatibles.txt](src/raft-peer-compatibles.txt) into [meta_to_meta.txt](meta_to_meta.txt). |
| [Makefile](Makefile) | Runs resolve, then generates the output tables. |

### Source Data (`src/`)

| File | Purpose |
|---|---|
| [src/min_compatible_versions.txt](src/min_compatible_versions.txt) | Raw version data from git tags. |
| [src/external-meta-min-compatibles.txt](src/external-meta-min-compatibles.txt) | Min server/client versions for each external `databend-meta` crate release. |
| [src/raft-peer-compatibles.txt](src/raft-peer-compatibles.txt) | Snapshot of databend-meta `raft-compat` output: raft peer range per metasrv version. |

### Generated Output

| File | Purpose |
|---|---|
| [generated/resolved_min_compatibles.txt](generated/resolved_min_compatibles.txt) | Resolved version table. Input for [scripts/find_compat.py](scripts/find_compat.py). |
| [query_to_meta.txt](query_to_meta.txt) | Compatible metasrv range for every query version. |
| [meta_to_query.txt](meta_to_query.txt) | Compatible query range for every metasrv version. |
| [meta_to_meta.txt](meta_to_meta.txt) | Compatible peer range for every metasrv version (raft inter-node protocol). |
