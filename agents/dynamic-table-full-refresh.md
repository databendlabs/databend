# Dynamic Table: explicit full refresh

Status: implemented for explicit `REFRESH DYNAMIC TABLE`. Scheduling is not implemented.

## Scope

`DYNAMIC_TABLE` is an independent engine backed by Fuse storage. It does not change the
existing materialized-view metadata, read path, or refresh path.

Supported: `CREATE DYNAMIC TABLE` over persistent FUSE base tables in one catalog, including
multi-table joins, self-joins and aggregates; `REFRESH DYNAMIC TABLE`; reads; `SHOW CREATE`;
`DESC`; `DROP`. Not supported and explicitly rejected: `TARGET_LAG`, `WAREHOUSE`,
`REFRESH_MODE = INCREMENTAL`, and non-FUSE or derived sources (view, stream, temp, another
dynamic table). Out of scope: dependency DAGs, cascading refresh, optimizer substitution.

The read contract is **live fallback**. An uninitialized or stale object executes its defining
query. A fresh object scans stored results. There is no hybrid delta read. An invalid binding or
a missing source is an error, not a reason to silently return stale rows.

## Metadata

Definition and refresh state live in the Dynamic Table's own `TableMeta.options`:

- `as_query`: the canonical, fully qualified defining SQL, produced by `ViewRewriter` so the
  meaning does not depend on the reader's current database.
- `dynamic_table_source_endpoints`: JSON array of `{table_id, table_seq, snapshot_location}`,
  sorted by `table_id` and deduplicated, describing exactly what the last successful refresh read.
- `dynamic_table_initialized`: whether stored data corresponds to that endpoint vector.
- `target_lag`, `refresh_mode`, `initialize`: the requested policy, retained for `SHOW CREATE`.

All of these are registered as **reserved** and **internal**, so a user cannot set or forge them
through `CREATE TABLE ... <option>` or `ALTER TABLE SET OPTIONS`, and they do not leak into
`SHOW CREATE TABLE` for ordinary tables.

This deliberately does not reuse MV source options, MV dependency records, or MV candidate APIs.
An earlier draft of this design used separate Meta records (`DynamicTableDefinition`,
`DynamicTableRefreshState`, a source→table reverse index, and protobuf v186). That scaffolding was
removed because the implementation never called it: table options already give atomic publication
through `upsert_table_option` with a `MatchSeq` fence, and they participate in the ordinary table
lifecycle. Reintroduce Meta records only when a feature actually needs them — a dependency DAG for
cascading refresh is the likely trigger, since that needs source→dependent lookups that table
options cannot answer.

Stored results are final query results, so the physical and logical schemas coincide. There is no
aggregate-state encoding and no `_mv_source_row_id`.

## Refresh protocol

1. Acquire the target table lock, evict cache, reload, and verify the engine.
2. Read the definition and capture the source endpoint vector.
3. Invalidate the checkpoint (`initialized = false`) **before** writing any data.
4. Execute the defining query and internally overwrite the target through
   `InsertInterpreter::try_create_refresh`, which is the only path allowed to write a
   read-only Dynamic Table and checks the exact target table id.
5. Re-read the endpoint vector. Publish `initialized = true` with that vector only if it is
   unchanged; otherwise leave the object uninitialized.

Step 3 is the ordering that matters. The data overwrite and the checkpoint update are two
transactions, so any failure between them must not leave stored data that a later read could match
against a checkpoint describing a different state. Without it, a source that later returns to an
older snapshot (flashback, revert, undrop) could make stale data compare equal and be served as
fresh. While uninitialized, reads fall back and stay correct.

Sources do not need CHANGE_TRACKING or retained delta history; refresh reads whole snapshots. An
empty result must still overwrite old rows.

### Known limitation: no snapshot pinning

Refresh does not pin source snapshots. It re-plans to collect endpoints and compares them before
and after the rebuild, refusing to publish when they differ. This is safe — a divergent result is
never advertised as fresh — but it is weaker than pinning, and a vector of individually observed
snapshots is not a cross-table transactional snapshot. Refresh consistency therefore equals the
ordinary query read contract, not a stronger guarantee.

This must be resolved before scheduling is added. Manual refresh hits the divergence window rarely;
an automatic scheduler would hit it constantly and would silently stop publishing on busy sources.

## Read protocol

`bind_dynamic_table` decides between two ordinary plan shapes at bind time:

- **Fresh** — every source's current `table_seq` and `snapshot_location` equals the stored
  endpoint. Binds the committed Fuse snapshot as a normal physical table scan.
- **Fallback** — uninitialized, or any endpoint differs. Expands `as_query` like an ordinary view,
  so it optimizes together with the enclosing query.

The stored definition is parsed with a fixed dialect (`Dialect::PostgreSQL`), because it is a
serialized AST rather than SQL in the reader's dialect; a session setting must not change what a
published object means. Fallback checks `check_view_loop` so an expansion cannot route back through
the same object, validates that the definition still produces the declared column count, and
preserves the declared output names either way. A missing source table is an error. A corrupt or
duplicated checkpoint is an error.

Dynamic Tables bypass the planner plan cache, for the same reason materialized views do: the cache
key does not include refresh state, so a cached fresh scan could otherwise survive a source commit
and keep serving stale rows. This was an actual observed bug, not a theoretical one.

## Read-only and lifecycle guards

`FuseTable::is_read_only` returns true for the engine, which makes `check_mutable` reject INSERT,
UPDATE, DELETE, MERGE, REPLACE and TRUNCATE, and also blocks `ALTER TABLE ADD/DROP COLUMN`.
Multi-table INSERT rejects the engine explicitly. `ENGINE = DYNAMIC_TABLE` is not accepted by the
`CREATE TABLE` engine parser at all, so the object cannot be forged that way. `SHOW CREATE TABLE`
emits a re-executable `CREATE DYNAMIC TABLE`. `system.tables` reports `DYNAMIC TABLE`, which
`information_schema.tables` inherits.

## Verification

`tests/sqllogictests/suites/base/05_ddl/05_0066_ddl_dynamic_table.test` covers fallback before
first refresh, fresh reads, staleness after source insert/delete/truncate, repeated refresh,
empty results, `ON_CREATE` versus `ON_SCHEDULE`, aggregates, self-joins, every read-only and
option-forging guard, rejected policies, dropped sources, and object-type reporting.

Regression suites run: `05_ddl`, `06_show`, `09_fuse_engine`, `01_system`. Registering the engine
changes `SHOW ENGINES` and `system.engines`, so those two goldens were updated.

Not run locally: the enterprise materialized-view suites under `suites/ee`, which need a license.
Those exercise shared code this branch touches (`is_fuse_backed_engine`, table option key sets,
`SHOW CREATE`, planner cache) and should be run in CI before merge.
