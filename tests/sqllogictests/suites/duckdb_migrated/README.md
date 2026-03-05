# duckdb_migrated

This suite stores sqllogictest files converted from upstream DuckDB by the `duckdb_convert_slt` skill.

Why this directory exists:
- Keep skill-generated migration output isolated from the existing `suites/duckdb` suite.
- Make baseline/backfill/incremental sync state explicit via `duckdb_migrated/.manifest.json`.

Path conventions:
- Manifest: `tests/sqllogictests/suites/duckdb_migrated/.manifest.json`
- Converted files: `tests/sqllogictests/suites/duckdb_migrated/sql/...`

Do not edit the manifest by hand. Use `skills/duckdb_convert_slt/scripts/sync_upstream.py`.
