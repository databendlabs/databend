# Databend Query

Databend Query is a Distributed Query Engine at scale.

- [`ast`](./ast/) contains the SQL parser and related parser tooling.
- [`catalog`](./catalog/) defines catalog-facing abstractions such as `Catalog`, `Database`, `Table`, and `TableContext`.
- [`codegen`](./codegen/) contains code generators used by query-side crates, including arithmetic result-type generation.
- [`config`](./config/) provides query-service configuration types and parsing.
- [`datavalues`](./datavalues/) is the legacy in-memory value/column representation layer; newer work continues to move toward [`expression`](./expression/).
- [`expression`](./expression/) is the core scalar expression framework, including expression representation, type checking, and evaluation.
- [`formats`](./formats/) handles data serialization and deserialization for external formats.
- [`functions`](./functions/) implements scalar functions, aggregate functions, and related function infrastructure.
- [`management`](./management/) contains query-side management utilities such as cluster and quota support.
- [`pipeline`](./pipeline/) implements the execution pipeline and scheduling framework for physical operators.
- [`script`](./script/) contains script execution support used by the query service.
- [`script_udf_support`](./script_udf_support/) contains optional script UDF integrations that are wired into the query service through the `script-udf` cargo feature.
- [`service`](./service/) is the `databend-query` service crate and runtime integration layer.
- [`settings`](./settings/) defines global and session settings.
- [`sql`](./sql/) contains SQL-side planning, binding, optimization, and related execution support. Read `./sql/README.md` for crate structure and shared optimizer test-support entry points.
- [`storage_stage_support`](./storage_stage_support/) contains optional stage-storage integrations that are wired into the query service through the `storage-stage` cargo feature.
- [`task_support`](./task_support/) contains optional task support integrations that are wired into the query service through the `task-support` cargo feature.
- [`storages`](./storages/) contains table engines and storage-layer integrations, including Fuse and related indexing components.
- [`users`](./users/) implements user, role, and access-control support.
- [`ee`](./ee/) contains enterprise query functionality.
- [`ee_features`](./ee_features/) contains enterprise feature modules used by the query layer.

## External Flight UDF connection diagnostics

`udf_attempt_failure` records DNS, TCP, and TLS outcomes for the initial dial of
that attempt. DNS `ok` means the resolver returned successfully, possibly from
cache. TLS `ok` for HTTPS also means HTTP/2 ALPN was accepted; it does not mean
the UDF received the request. `redial_started=true` means tonic started another
dial, so do not attribute the RPC failure to the initial peer alone. The
diagnostics do not send extra traffic. `udf_call_recovered` is emitted at INFO
only when a retry succeeds. Match it to earlier failures using `query_id`,
`node_id`, and `call_id`; `attempt` is the successful attempt number.

The actual initial TCP peer IP (`initial_peer_ip`) is in a separate
`udf_attempt_peer` event. Join on `query_id`, `node_id`, `call_id`, and `attempt`.
A failed TCP dial does not have a connected peer. The peer can be a load balancer
or proxy, not necessarily the UDF backend.

Peer events are off by default. Query capture, log history, stderr, and trace
export exclude them. Operators may append `databend::log::udf_peer=WARN` to
`log.file.level` or `log.otlp.level` on an enabled sink **only if** its storage,
forwarding, and access controls are restricted to authorized operators. For the
file sink, use `json` or `text` so that structured fields are retained. Remove
the directive after diagnosis. Neither event includes the URL path or query,
headers, DNS answer lists, or UDF data.
