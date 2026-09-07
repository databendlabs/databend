# Dynamic Table: explicit full refresh

Status: implemented for explicit `REFRESH DYNAMIC TABLE`. Scheduling is not implemented. Not yet
production-ready; see the readiness section.

## Scope

`DYNAMIC_TABLE` is an independent engine backed by Fuse storage. It does not change the existing
materialized-view metadata, read path, or refresh path.

Supported: `CREATE DYNAMIC TABLE` over persistent FUSE base tables in one catalog, including
multi-table joins, self-joins and aggregates; `REFRESH DYNAMIC TABLE`; reads; `SHOW CREATE`;
`DESC`; `DROP`. Rejected rather than silently ignored: `TARGET_LAG`, `WAREHOUSE`, and non-FUSE or
derived sources (view, stream, temp, another dynamic table). Out of scope: dependency DAGs,
cascading refresh, optimizer substitution.

## Read contract

A Dynamic Table is read exactly like a physical table. Every query serves the result materialized
by the last refresh, however stale that is. This matches Snowflake: staleness is observable to
readers, which is the entire premise of a lag target. There is no fallback to the defining query,
no delta compensation, and no hybrid read.

The consequence is deliberate: after a source commit, the Dynamic Table keeps returning its
previous contents until someone refreshes it. That is what distinguishes it from a view. Because
there is no scheduler yet, **staleness is unbounded** — if nobody runs `REFRESH`, the object serves
old rows indefinitely with no automatic correction.

Refresh is always a full rebuild, so there is no refresh mode to choose. `CREATE` always
materializes, so there is no initialization mode either. Neither `REFRESH_MODE` nor `INITIALIZE` is
part of the grammar.

### No automatic query rewriting

A Dynamic Table is never used by the optimizer to accelerate a query against its base tables. You
must name it explicitly. This is a different mental model from a materialized view's transparent
acceleration: pointing a BI tool at the base tables will not route to the Dynamic Table.

This is structural, not incidental. The rewrite rule `RuleTryApplyMaterializedView` reads
`Metadata::get_materialized_view_candidates`, whose only writer is `binder/ddl/materialized_view.rs`,
fed by `SourceTableMVIdent` Meta records. Dynamic Table creation writes no such record and touches
no MV API, so a Dynamic Table cannot enter the candidate set.

Two things follow. The planner's MV fingerprint path, which invalidates cached plans for ordinary
table reads when a dependent MV changes, does not need to account for Dynamic Tables — no plan can
be rewritten to read one. And a Dynamic Table needs no plan-cache bypass: its read is a plain
physical scan whose shape does not depend on refresh state.

## Metadata

Only the definition is persisted, in the Dynamic Table's own `TableMeta.options`:

- `as_query`: the canonical, fully qualified defining SQL, produced by `ViewRewriter` so the meaning
  does not depend on the reader's current database.

It is registered **reserved** and **internal**, so a user cannot forge it through
`CREATE TABLE ... <option>` or `ALTER TABLE SET OPTIONS`, and it does not leak into
`SHOW CREATE TABLE` for ordinary tables.

No refresh checkpoint is stored. Because reads never consult one, an endpoint vector or an
initialized flag would have no runtime reader. An earlier draft of this design stored both, plus
separate Meta records (`DynamicTableDefinition`, `DynamicTableRefreshState`, a source→dependent
reverse index, protobuf v186). All of it was removed as dead code.

That removal costs one capability worth recording: there is no reverse index from a source table to
the Dynamic Tables reading it. Table options answer "what does this object read", not "who reads
this table". A dependency DAG for cascading refresh needs the latter and is the likely trigger for
reintroducing Meta records. Adding it later means backfilling existing objects, so it is cheaper to
add when scheduling work begins than to retrofit afterwards.

Stored results are final query results, so the physical and logical schemas coincide. There is no
aggregate-state encoding and no `_mv_source_row_id`. Sources need no CHANGE_TRACKING and no
retained delta history.

## Refresh protocol

1. Acquire the target table lock, evict cache, reload, and verify the engine.
2. Plan the stored definition, then build an overwriting `Insert` plan **structurally** from the
   target's own `TableInfo` and execute it through `InsertInterpreter::try_create_refresh`, the
   only path allowed to write a read-only Dynamic Table, which checks the exact target id.

Step 2 is built structurally rather than by formatting an `INSERT OVERWRITE` string. The tokenizer
forbids a backtick inside backtick-quotes (`` `[^`]*` ``), but permits one inside double-quotes,
and PostgreSQL dialect quotes identifiers with `"`. So `CREATE DATABASE "bt`db"` is reachable, and
formatting that name back into backticks produced
`` INSERT OVERWRITE `default`.`bt`db`.`dt` ... `` — the name broke out of its quoting in a write
path, error 1005. This was reproduced end to end, then fixed. Only the stored definition, which the
system itself serialized, is parsed here.

`INSERT OVERWRITE` lands as a single Fuse commit, so a reader observes either the previous contents
or the new ones, never a mixture, and a failed refresh leaves the previous contents intact. An empty
result still overwrites old rows.

`CREATE` is all-or-nothing. Since reads always serve stored data, an object that exists but was
never populated would answer queries with zero rows instead of an error — silently wrong. So if the
initial refresh fails, the table is dropped and the failure is surfaced. If that rollback itself
fails, the original error is reported with the leaked table named, rather than hidden.

### Known limitation: no snapshot pinning

Refresh does not pin source snapshots; it plans and reads whatever each source currently exposes.
A vector of individually observed snapshots is not a cross-table transactional snapshot, so refresh
consistency equals the ordinary query read contract rather than a stronger guarantee.

This should be resolved before scheduling is added, since a scheduler over busy sources amplifies
the window.

## Read-only and lifecycle guards

`FuseTable::is_read_only` returns true for the engine, which makes `check_mutable` reject INSERT,
UPDATE, DELETE, MERGE, REPLACE and TRUNCATE, and also blocks `ALTER TABLE ADD/DROP COLUMN`.
Multi-table INSERT rejects the engine explicitly. `ENGINE = DYNAMIC_TABLE` is not accepted by the
`CREATE TABLE` engine parser at all, so the object cannot be forged that way. `SHOW CREATE TABLE`
emits a re-executable `CREATE DYNAMIC TABLE` with no refresh policy, since there is none to echo.
`system.tables` reports `DYNAMIC TABLE`, which `information_schema.tables` inherits.

Dropping a source does not break reads: the stored result is self-contained and keeps being served.
Refresh is what fails, which is where the user learns the pipeline is broken.

## Production readiness

The read semantics are sound and directly tested. The feature is **not** production-ready. The
suites below are broad but shallow: none exercise concurrent sessions, process failure, large
tables, or awkward identifiers.

| Gap | Impact |
| --- | --- |
| No operational visibility | No system view for refresh history, duration, error, or in-flight state; no row cap, timeout, or progress. Makes every other gap hard to diagnose, and makes unbounded staleness invisible. `system.tables.updated_on` is currently the only signal, and for a Dynamic Table it does equal the last refresh time. |
| Unbounded staleness | With no scheduler, a never-refreshed object serves old rows forever. Nothing reports how stale it is. |
| Refresh serializes against itself but not against source DDL | Refresh takes the target lock, not source locks. |
| No snapshot pinning | See above. |

Suggested order: a `system.dynamic_tables` view (makes the rest diagnosable) → snapshot pinning →
scheduling last, since it depends on all of the above.

A note for whoever builds that view: `is_fresh` cannot be reported as a fact any more. The refresh
checkpoint was removed, so the only available basis would be comparing the object's Fuse snapshot
timestamp against its sources'. That misreports in both directions — a source compaction creates a
newer snapshot with unchanged logical content (false stale), and a flashback moves a source to an
older snapshot (false fresh). Reporting refresh *time* is factual; reporting freshness needs a
deliberately reintroduced checkpoint.

## Verification

`tests/sqllogictests/suites/base/05_ddl/05_0066_ddl_dynamic_table.test` (96 assertions) covers the
stale-read contract (a source commit does not change what the object returns), refresh publishing
new rows, idempotent refresh, empty results, aggregates, self-joins, `CREATE` rollback on a failed
initial refresh, reads surviving a dropped source while refresh fails, a database name containing a
backtick, every read-only and option-forging guard, rejected policies, `SHOW CREATE` round-trip, and
object-type reporting.

Two assertions were verified non-vacuous by reverting their fix and observing the failure: the
`CREATE` rollback (leaks the empty table), and the backtick identifier (error 1005 on the
string-formatted overwrite). The stale-read contract was additionally confirmed against a live
server with `EXPLAIN`: the plan is a plain `TableScan` of the object itself, unchanged by a source
commit, and querying the base tables produces a join with no reference to the Dynamic Table.

Regression suites at this commit: `05_ddl` 2273, `20+_others` 685, `01_system` 137, `06_show` 396.
`01_system` and `06_show` fail when run immediately after the full `05_ddl` directory; this was
confirmed pre-existing by reproducing it with this branch's test file removed entirely.

Not run locally: the enterprise materialized-view suites under `suites/ee`.
`CREATE MATERIALIZED VIEW` returns error 1006 (EE-only) in this environment. They exercise shared
code this branch touches (`is_fuse_backed_engine`, table option key sets, `SHOW CREATE`) and should
pass in CI before merge.
