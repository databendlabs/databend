# Databend threat model

## Project and attack surfaces

Databend is an open-source, cloud-native data warehouse written in Rust, designed as an alternative to
Snowflake: separated storage and compute on any object store, with stages, `COPY INTO`, time travel,
streams, tasks, and RBAC with row access policies and data masking. A deployment is one or more
`databend-query` nodes (SQL frontend, planner, vectorised executor, storage access) backed by a
`databend-meta` cluster (Raft-replicated metadata: tenants, users, roles, grants, databases, tables,
stages, UDFs) and an object store holding table data in the Fuse format.

A `databend-query` process serves the single tenant named by its `tenant_id` configuration. A
`databend-meta` cluster and an object store may each be shared by several tenants, with metadata keyed
per tenant and table data kept under a per-tenant storage root. Treat SQL text, protocol framing, HTTP headers and bodies, stage and connection URIs, staged files,
external table metadata, JWT claims, and UDF bodies as untrusted. The primary attacker is an
authenticated user with limited grants inside one tenant; an authenticated user is not a tenant
administrator, and a tenant's process is not the cluster.

Prioritize privilege checks on every plan, row access policy and data masking on every read path,
authentication on the HTTP, MySQL and Arrow Flight SQL handlers, anything that lets a process act on a
tenant other than its configured `tenant_id` (meta requests, storage paths that escape the configured
root, `ATTACH TABLE` and shared-storage metadata), exposure of stage/connection credentials, session
tokens, or credentials the process itself holds, outbound requests driven by user-supplied
URIs, parsing of attacker-controlled files and table metadata, and the embedded UDF runtimes.

## Components and trust boundaries

- `src/query/service/src/servers/http/` (`:8000`), `servers/mysql/` (`:3307`, on `opensrv-mysql`),
  `servers/flight_sql/` (`:8900`): client-facing protocol handling, session and token lifecycle
  (`http/v1/session/`), stage upload and streaming load, catalog browsing. `servers/flight/` (`:9091`)
  is the node-to-node API and must still authenticate peers. `servers/admin/` (`:8080`) and metrics
  are operator-only and expected to be firewalled.
- `src/query/service/src/auth.rs`, `src/query/users/` (users, roles, grants, password and network
  policies, `jwt/` for JWKS auth, `visibility_checker.rs`), `src/query/service/src/sessions/
  session_privilege_mgr.rs`, and `src/query/service/src/interpreters/access/privilege_access.rs`: the
  privilege gate every plan passes through. `src/query/ee/` and `src/query/ee_features/` add
  `data_mask`, `row_access_policy`, `storage_encryption`, `data_sharing`, `attach_table`.
- `src/query/ast/` (hand-written parser), `src/query/sql/` (binder, optimizer, planner),
  `src/query/expression/`, `src/query/functions/` (built-in scalar/aggregate/table functions, JSONB,
  regexp, geo, decimal): SQL semantics, recursion depth, system tables and table functions that may
  bypass the privilege gate or leak other users' state.
- `src/query/storages/stage/` (`COPY INTO`, `INSERT ... FROM @stage`, streaming load, `INFER_SCHEMA`),
  `src/query/formats/` (CSV/TSV/NDJSON/JSON), `src/query/storages/parquet/`, `storages/orc/`, Avro
  in `storages/stage/src/avro_utils.rs`, `src/common/compress/`, and the external catalogs in
  `storages/iceberg/`, `storages/paimon/`, Hive: attacker-controlled bytes reach these decoders.
- `src/common/storage/` (OpenDAL wrapper, URI and credential resolution) and
  `src/query/sql/src/planner/binder/location.rs`: user-supplied stage/connection/location URIs,
  including the `fs` scheme, decide what the node fetches and with which credentials. A path that
  reads the local filesystem, a cloud metadata endpoint, or storage outside the configured root is a
  credential or isolation issue, not just an SSRF.
- `src/query/storages/fuse/` and `src/query/storages/common/table_meta/`: snapshot, segment, block
  and index decoding. With `ATTACH TABLE` or shared storage this metadata may come from another trust
  domain. Also the `unsafe`-dense engine code in `src/query/expression/`, `src/common/column/`,
  `src/common/hashtable/`, `src/common/base/`, `src/query/pipeline/`.
- `src/query/script_udf_support/`: JavaScript (`rquickjs`) and WASM (`arrow-udf-runtime`) UDFs
  written by ordinary database users. Python UDFs are behind the non-default `python-udf` feature.
- `src/meta/`: gRPC service, `proto-conv` deserialisation. The meta cluster is shared by all tenants;
  a query process must only read and write keys under its own tenant, and a value written by one
  node is read by every node in that tenant.
- `src/query/service/src/sessions/session.rs` (the conditions under which a session's tenant may
  differ from the configured `tenant_id`) and
  `src/query/service/src/interpreters/access/management_mode_access.rs`: with default configuration a
  session must never be able to change its tenant.

State whether a finding needs operator access, a config change, direct object-store access, or a
licence-gated feature. Do not grant those to an ordinary SQL user. `--internal-enable-sandbox-tenant`
makes the `X-Databend-Tenant` header trusted on purpose so one test server can emulate many tenants;
only treat it as a bug when the header is honoured without that flag.

## Offline environment and reproduction

Source, toolchain, Cargo dependencies and debug build artifacts are in the image at `/src`;
`CARGO_NET_OFFLINE=true` is set. Binaries are `/src/target/debug/databend-query`, `databend-meta`,
`databend-metactl`, `databend-sqllogictests`. The build used default features (`simd`, `script-udf`,
`task-support`, `storage-stage`); record the feature set and configuration for each finding.

Start a standalone meta + query on local `fs` storage under `./.databend`:

```bash
cd /src
bash ./scripts/ci/deploy/databend-query-standalone.sh
```

This passes `--internal-enable-sandbox-tenant`. For default tenant handling run
`target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml` directly. Ports:
HTTP `8000`, MySQL `3307`, Flight SQL `8900`, admin `8080`, meta gRPC `9191`. The config defines user
`root` with `no_password` from `127.0.0.1`; create more users with `CREATE USER` to test RBAC.

```bash
curl -u root: -H 'Content-Type: application/json' -d '{"sql":"select version()"}' http://127.0.0.1:8000/v1/query
mysql -h127.0.0.1 -P3307 -uroot
```

Staged files: `CREATE STAGE s;` then `PUT` to `/v1/upload_to_stage` with headers
`x-databend-stage-name: s` and `x-databend-relative-path: dir/`, or `CREATE STAGE s
URL='fs:///tmp/data/'` to read a local directory (`STORAGE_ALLOW_INSECURE=true` is set).

The following suites ran during image construction and can be rerun offline:

```bash
cargo test --frozen --lib -p databend-common-users -p databend-common-ast -p databend-common-io \
  -p databend-common-formats -p databend-common-column -p databend-common-hashtable
target/debug/databend-sqllogictests --handlers http,mysql --run_dir 00_dummy --enable_sandbox
```

Suites live in `tests/sqllogictests/suites/`; `--run_dir` takes a directory name such as `05_ddl`.
Other crates compile on demand with `cargo test --frozen -p <crate>`; keep `CARGO_BUILD_JOBS=2` on the
audit VM, and prefer `-p` to `--workspace`. Relinking `databend-query` after a patch takes a few
minutes. Logs are in `.databend/logs_1/` (query) and `.databend/logs1/` (meta).

For request-level findings exercise the running server over HTTP or MySQL, not only a helper function.
External object stores, Iceberg/Hive catalogs, JWKS endpoints and UDF servers are unavailable; use
local fixtures.

## Severity and report evidence

Acting on another tenant's metadata or storage from a tenant process, authentication bypass on any
handler, disclosure of credentials the process holds, UDF sandbox escape, and memory
corruption reachable from SQL, a staged file, or table metadata merit critical severity. Privilege
escalation past the caller's grants, row access policy or data masking bypass, credential or session
token disclosure, SSRF with the node's credentials, and silent corruption of committed data generally
merit high. For process abort, unbounded allocation or hangs from crafted input, consider
authentication, amplification, and how ordinary the triggering input looks rather than a fixed rating.
Operator-only surfaces (admin API, metrics, config, CLI), licence-gating, vulnerable dependencies
Databend does not reach, and anything that only affects `tests/`, `scripts/` or CI are low or out of
scope.

Include the source commit, the user and role you ran as with `SHOW GRANTS`, a minimal offline
reproducer (SQL script, `curl`/`mysql` transcript, staged file plus the triggering statement, or a Rust
test), expected versus actual behaviour, and for memory-safety findings the panic or sanitizer output
with `RUST_BACKTRACE=1`. Prefer a minimal patch with a regression test: a `.test` file under
`tests/sqllogictests/suites/` for SQL-visible behaviour, or a unit test in the affected crate.
Deduplicate reports sharing a root cause while listing every demonstrated entry point.

Follow `SECURITY.md`: report privately to `security@databend.com`. Do not publish exploit details in
public issues.
