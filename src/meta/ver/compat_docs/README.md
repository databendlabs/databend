# compat_docs

Compatible version ranges between databend-query and databend-meta-service.

## For Users

Pre-calculated compatible version ranges:

- [query_to_meta.txt](query_to_meta.txt) — compatible metasrv versions for each query version.
- [meta_to_query.txt](meta_to_query.txt) — compatible query versions for each metasrv version.

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


### Scripts

| File | Purpose |
|---|---|
| [scripts/extract_from_git.py](scripts/extract_from_git.py) | One-time. Extracts version constraints from git tags into [src/min_compatible_versions.txt](src/min_compatible_versions.txt). |
| [scripts/resolve_versions.py](scripts/resolve_versions.py) | Resolves CalVer (e.g. `260205.0.0`) to repo tag versions. Writes [generated/resolved_min_compatibles.txt](generated/resolved_min_compatibles.txt). |
| [scripts/find_compat.py](scripts/find_compat.py) | Prints compatible version range for a given query or metasrv version. |
| [Makefile](Makefile) | Runs resolve then generates both output files. |

### Source Data (`src/`)

| File | Purpose |
|---|---|
| [src/min_compatible_versions.txt](src/min_compatible_versions.txt) | Raw version data from git tags. |
| [src/external-meta-min-compatibles.txt](src/external-meta-min-compatibles.txt) | Min server/client versions for each external `databend-meta` crate release. |

### Generated Output

| File | Purpose |
|---|---|
| [generated/resolved_min_compatibles.txt](generated/resolved_min_compatibles.txt) | Resolved version table. Input for [scripts/find_compat.py](scripts/find_compat.py). |
| [query_to_meta.txt](query_to_meta.txt) | Compatible metasrv range for every query version. |
| [meta_to_query.txt](meta_to_query.txt) | Compatible query range for every metasrv version. |
