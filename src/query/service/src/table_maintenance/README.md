# Best-effort automatic table maintenance

## Contract

For persistent tables, automatic Compact still finishes before the writing statement
returns, with the existing triggers. Recluster, asynchronous index Refresh and automatic
Analyze run independently in the node-local mailbox. Temporary tables retain their
session-local synchronous path. The deprecated `table_hook_mode` option is accepted but
does not make persistent-table maintenance synchronous.

The mailbox is **best-effort**, not an event-delivery or eventual-convergence guarantee:

- Notifications for an existing `(tenant, catalog, table_id)` merge, even at capacity.
- A new table at capacity is rejected immediately, without delaying a committed write.
- Node shutdown discards queued notifications and in-memory progress. There is no durable
  queue, owner lease, task takeover or periodic compensation scan.
- Each action stops after 8 errors without a successful commit/completion, or 10 minutes
  of scanning/deferral without a commit. Notifications merged into that attempt and
  metadata-only scan steps do not reset its budget. A later notification can start another
  attempt. Successful committed progress or completion resets the failure/deferral budget.
  The no-commit timer begins at the first scan/deferral, not while waiting in the queue.
- An action's context creation is limited to 60 seconds and each execution unit to 5 minutes.
  Cancellation interrupts its context, allows up to 3 seconds for normal pipeline cleanup,
  and tears down any remaining local exchange environment. These limits require async work
  to yield; they cannot preempt an arbitrary synchronous CPU operation.
- Recluster is triggered by successful automatic Compact, not by every write. It remains
  distributed and yields between rounds. Refresh and Analyze do not wait for convergence.
- `query.table_maintenance_recluster_accumulation_window_secs` defaults to 600 (10 minutes).
  The first dirty notification starts the window; merged notifications do not postpone it.
  Demand arriving during a running generation requests another generation, whose window
  starts after the current generation ends. Rounds within one generation do not repeat this
  wait. Zero makes a generation immediately eligible; it does not disable maintenance.
  Retry backoff and the short round/time-slice cooldowns are independent of this window.
- Background Recluster builds both SYNC and ASYNC indexes for newly rewritten blocks:
  Bloom/Ngram, Spatial and Vector are rebuilt; compatible inverted indexes are merged
  only when all source metadata and row lineage permit it, otherwise they are rebuilt.
  Data and index references are published in one snapshot commit. Any builder/merge failure
  aborts that round rather than publishing a block missing an expected index.
- Indexed Recluster uses the same local/remote admission and memory group as other
  maintenance. Its planner leaves additional input-byte headroom for index writers (more
  for inverted/vector indexes). This is a heuristic, not an index-size guarantee: the hard
  tracked memory limit and bounded failure/retry policy remain authoritative. No sort spill
  or remote load probe is enabled to hide a too-large task.
- Before publication, and again on each OCC rebase, schema, index definitions, Bloom
  options and the Recluster key must still match those used to build the artifacts.
  Segment claims and the normal commit lock are unchanged. Unpublished objects are left
  for normal vacuum; cancellation or an unknown commit result must not blindly delete
  potentially committed data/index objects.
- A Recluster commit or error does not register Refresh or re-arm Analyze. Write-triggered
  Refresh, historical coverage and explicit index Refresh remain independent. Recluster
  also does not advance their frozen scopes or covered baselines: untouched old blocks
  may still need Refresh, and a later write may conservatively fall back to FullCheck.

Later writes can request maintenance again. Without another trigger, a cold table may
remain unmaintained indefinitely. Explicit `ALTER TABLE ... RECLUSTER`, `REFRESH ... INDEX`
and `ANALYZE TABLE` remain the user-controlled repair paths, with their own error handling.

Missing indexes do not all have the same query behavior: Ngram/other pruning fallbacks
must not discard rows, while a required inverted index that is missing or out of date
returns `RefreshIndexError` and requires Refresh. Best-effort maintenance does not promise
that such an indexed query succeeds immediately after an acknowledged write.

None of this weakens storage publication: table-ID checks, lifecycle locks, segment claims,
OCC, current schema/index-definition validation and immutable index artifacts still protect
concurrent writes and old snapshots. Scheduling ownership is not a replacement for them.

## Resource limits and shutdown

`query.table_maintenance_max_memory_usage` bounds the tracked maintenance memory group
(default 2 GiB). Coordinator and remote contexts reserve a share before planning;
`query.table_hook_async_max_concurrency` determines the number of shares. Thread and IO
settings are constrained independently on every participating node. The default is 2
threads per unit and 1024 pending/running table keys. Automatic sort spill is disabled
until a separate maintenance spill budget exists.

These are not CPU-time, storage-bandwidth or tenant/workload isolation guarantees. Frozen
Recluster scope/carry and snapshot bookkeeping can still be expensive on large tables;
measure RSS, tracked memory and metadata planning costs, not only the queue's table count.
An oversized task may fail its bounded attempt rather than finish automatically.

Production shutdown closes both mailbox and remote maintenance admission before stopping
service dependencies. Registered shared contexts are interrupted, including derived fragment
contexts. The scheduler's final wait uses at most the remaining shutdown grace, capped at
3 seconds; shutdown never waits for all pending maintenance to complete.

## Observability

Metric labels contain only bounded action/outcome/reason values, never table, user or query
IDs. The exporter may prefix names with `databend_` and counters with the usual `_total` suffix.

- `table_maintenance_rejected{reason}`: capacity or shutdown rejections.
- `table_maintenance_stopped{action,reason}`: attempts abandoned without claiming coverage.
- `table_maintenance_mailbox_tables`: admitted queued and running table keys.
- `table_maintenance_running_units`: currently executing units.
- `table_maintenance_units{action,outcome}` and `table_maintenance_unit_milliseconds{action}`:
  completed/progress/scan/deferred/error/cancelled unit counts and elapsed execution time.
- `table_maintenance_last_success_timestamp_seconds{action}`: last completed/committing unit
  (not scan-only).
- `table_maintenance_reserved_memory_bytes`: admitted coordinator/remote reservations on this node.

Logs associate the source query ID and table ID with the independent execution query ID.
Refresh scope and Recluster round logs distinguish actual progress from completion. Metrics
show node-level health, not durable per-table progress or guaranteed completion.

## Validation

Scheduler/admission/retry tests live in the trailing test modules in
`../interpreters/hook/table_hook_{scheduler,task}.rs`. Context cancellation and reservation
shutdown tests live beside this implementation. Storage/index publication integration tests
live in `src/query/service/tests/it/indexes/` and
`src/query/service/tests/it/storages/fuse/operations/clustering.rs`.
The licensed MV post-commit regression lives in `src/query/ee/tests/it/materialized_view.rs`.
Identity regressions verify that global-only settings (including enterprise license) reload
from current node/tenant policy rather than being replayed as illegal session overrides;
replayable semantic settings remain frozen values and resource overrides are excluded.

The private-service regression `tests/suites/1_stateful/02_query/02_0013_async_table_hook.py`
uses bounded coverage polling, not fixed sleeps or foreground-return assumptions. It stops
only the exact service process handles it started. Normal SQL tests that depend on automatic
index publication must likewise wait for observable coverage or explicitly refresh.

### Local resource probe (2026-11-12)

A debug-build smoke used one node, filesystem storage, 2 threads per query/unit, the default
2 GiB maintenance budget, a one-table mailbox, and automatic Compact disabled to isolate
Refresh. The target contained 100,000 rows across 500 segments (`ROW_PER_BLOCK=200`,
`BLOCK_PER_SEGMENT=1`). Creating ASYNC inverted/Ngram indexes and appending one row triggered
coverage of all 501 segments. A separate foreground query repeatedly calculated
`sum(siphash64(number) % 10000)` over `numbers(1000000)`, with 3 warmups and 30 samples in each
phase; the loaded phase overlapped an admitted maintenance request.

| Observation | Result |
| --- | --- |
| Idle foreground median / maximum of 30 samples | 106.5 / 113.1 ms |
| Foreground during Refresh median / maximum of 30 samples | 108.3 / 119.7 ms |
| Triggering append response | 85 ms |
| Another table's append while mailbox full | 91 ms, committed; capacity-rejection counter incremented |
| Final coverage / row count | all index artifacts covered; 100,001 rows |
| Idle mailbox / reserved memory after completion | 0 tables / 0 bytes |
| Sampled RSS before / peak / after | 2152 / 2282 / 2262 MiB |
| SIGTERM with another Refresh still admitted | normal exit in 2.22 seconds |

A separate two-node cancellation smoke waited for an automatic remote Recluster fragment
and a nonzero worker reservation, then sent SIGTERM to the exact coordinator process.
The coordinator exited normally in 2.12 seconds; worker maintenance reservations and mailbox
returned to zero. The surviving node still read 400,004 rows with `sum(id)=9,999,800,006`.
This tests graceful coordinator cancellation, not guaranteed recovery after an ungraceful
network partition or process crash.

This checks obvious foreground blocking and resource/lifecycle regressions, not a production
latency acceptance threshold. Thirty samples are insufficient for a stable tail-latency
estimate; debug execution, filesystem caches and allocator retention affect both latency and
RSS. RSS did not return to its initial baseline, and zero reservations do not prove absence
of memory leaks. Million-segment metadata scaling, sustained multi-tenant pressure and
bandwidth/CPU isolation remain unvalidated.
