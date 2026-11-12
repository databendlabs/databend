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
- Each action stops after 8 consecutive errors, or 10 minutes of deferral without progress.
  Notifications merged into that attempt do not reset its failure budget. A later notification
  can start another attempt. Successful progress resets the failure/deferral budget.
- An action's context creation is limited to 60 seconds and each execution unit to 5 minutes.
  Cancellation interrupts its context, allows up to 3 seconds for normal pipeline cleanup,
  and tears down any remaining local exchange environment. These limits require async work
  to yield; they cannot preempt an arbitrary synchronous CPU operation.
- Recluster is triggered by successful automatic Compact, not by every write. It remains
  distributed and yields between rounds. Refresh and Analyze do not wait for convergence.

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
  completed/progress/deferred/error/cancelled unit counts and elapsed execution time.
- `table_maintenance_last_success_timestamp_seconds{action}`: last completed or progressing unit.
- `table_maintenance_reserved_memory_bytes`: admitted coordinator/remote reservations on this node.

Logs associate the source query ID and table ID with the independent execution query ID.
Refresh scope and Recluster round logs distinguish actual progress from completion. Metrics
show node-level health, not durable per-table progress or guaranteed completion.

## Validation

Scheduler/admission/retry tests live in the trailing test modules in
`../interpreters/hook/table_hook_{scheduler,task}.rs`. Context cancellation and reservation
shutdown tests live beside this implementation. Storage/index publication integration tests
live in `tests/it/indexes/` and `tests/it/storages/fuse/operations/clustering.rs`.

The private-service regression `tests/suites/1_stateful/02_query/02_0013_async_table_hook.py`
uses bounded coverage polling, not fixed sleeps or foreground-return assumptions. It stops
only the exact service process handles it started. Normal SQL tests that depend on automatic
index publication must likewise wait for observable coverage or explicitly refresh.
