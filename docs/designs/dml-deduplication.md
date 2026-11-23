# DML deduplication contract

A deduplication label is a client retry token, not a Meta key. If a response is
lost after a durable write commits, a retry with the same token should not
repeat the write. Query derives the key from the authenticated execution context
and authorized target. The marker commits atomically with the durable effect.

This document distinguishes the current behavior from the target contract.

## Token entry points

| Entry point | Compatibility rule |
| --- | --- |
| SQL `SET_VAR` hint and statement `SETTINGS` clause | Keep both as statement-scoped inputs. Prefer these forms for SQL clients. |
| HTTP `X-DATABEND-DEDUPLICATE-LABEL` | Keep it as a query-scoped input. A request header must not create a session or tenant default. |
| SQL `SET deduplicate_label` and HTTP `session.settings` | Keep as session-scoped compatibility inputs. Clients should clear a session label after the intended write and its retries. The empty string means no label. |
| SQL `SET GLOBAL deduplicate_label` | Reject it. Previously stored global values are ignored on load; nothing needs to be migrated. Use a statement or request token instead. |

Retain existing precedence: statement settings override the query header,
which overrides session settings. Within SQL, a `SETTINGS` clause takes
precedence over `SET_VAR` hints when both appear. Apply the same token
validation to all accepted forms. Do not inherit an outer label as a separate
retry token for each nested statement in a script or procedure; a client
retrying a whole script needs a separate contract. Explicit labels on individual
statements follow the rules below.

## Supported operations

| Operation | Current behavior | Target behavior |
| --- | --- | --- |
| Single-target `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `MERGE`, and `COPY INTO <table>` on a persistent Fuse-backed table | The interpreter can check a label before execution, and the Fuse commit path can record it. A label is currently shared across statement kinds and targets within the tenant. | Support a bounded token scoped to the authenticated principal, catalog, target, and operation kind. A retry must use the same token for the same logical operation. A successful retry may return an already-committed outcome rather than replaying the original result. |
| Supported DML that consumes a stream but changes no table rows | A stream-offset-only commit can record a marker. | Keep this path: advancing an offset is a durable effect. A statement with no durable effect leaves no persistent marker. |
| Supported statements inside an explicit transaction | The open transaction checks buffered labels. Only statements with persistent updates contribute markers at commit. | Keep duplicate handling local to the open transaction until commit and publish only markers tied to durable effects. Rollback leaves no persistent marker. A conflicting marker must not cause a transaction containing new work to report success. A statement token does not identify or replay the whole transaction. |
| Temporary-table-only writes | No persistent marker is written. A label is buffered only for duplicate checks within an open transaction. | Reject a token before execution until a session-local retry contract exists. Never persist a marker for a temporary-table-only write. |
| `COPY INTO <location>` | It consults the same lookup as table writes, but the stage sink does not record a matching marker. | Reject a token before execution until an export-specific contract exists. A table-write marker does not prove that an external file was written. |
| Multi-table `INSERT`, other storage engines without marker support, and other statements without a retry contract | Not consistently covered; for example, multi-table `INSERT` does not pass a label to its commit plan. | Reject an explicit token on unsupported operations before execution. Do not silently run an unsupported labeled write as an ordinary write. |

The supported table-write contract covers the durable commit, not all work done
while preparing it. A concurrent retry can still do speculative work; only one
attempt may publish the durable effect while its marker exists. Keep existing
file-level `COPY` deduplication separate from statement retry tokens.

## Compatibility and rollout

- Cross-target or cross-statement-kind suppression is not a retry guarantee.
  Some existing tests depend on this behavior (for example, reusing an
  `INSERT` label on `REPLACE`). Update those tests when switching namespaces,
  and keep a same-operation retry test across a reconnect.
- Do not treat tenant-only markers as proof that a newly scoped operation
  committed. Plan the namespace cutover with clients that have outstanding
  labeled writes; reconcile any uncertain outcome before retrying it.
- Continue to use normal authorization for the target even when a marker
  exists. Do not promise exact-request matching for operations where a stable
  request fingerprint is unavailable.

## Implementation references

- [Statement settings and hint resolution](../../src/query/sql/src/planner/statement_settings.rs)
- [HTTP session settings](../../src/query/service/src/servers/http/v1/query/http_query.rs)
  and [query label header application](../../src/query/service/src/servers/http/v1/query/http_query_context.rs)
- [Session and global settings rules](../../src/query/settings/src/settings_global.rs)
- [Query deduplication check](../../src/query/service/src/interpreters/common/util.rs)
  and [Fuse table commit](../../src/query/storages/fuse/src/operations/commit.rs)
- [Stream-offset-only commit](../../src/query/storages/fuse/src/operations/common/processors/sink_commit.rs)
  and [existing regression](../../tests/sqllogictests/suites/ee/06_ee_stream/06_0018_stream_noop_dedup_label.test)
- [Explicit-transaction retry test](../../tests/sqllogictests/suites/http_handler/transaction/txn_dedup_label.test)
  and [HTTP stage-load retry test](../../tests/suites/1_stateful/07_stage_attachment/07_0002_insert_with_stage_deduplicate.sh)
