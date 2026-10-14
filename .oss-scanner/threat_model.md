# Threat model: Databend

## What this project is

Databend is an open-source, cloud-native data warehouse written in Rust and designed as a drop-in
alternative to Snowflake: the same separation of storage and compute, the same SQL surface (stages,
`COPY INTO`, time travel, streams, tasks, RBAC with row access policies and data masking), and the
same multi-tenant deployment model, running on any object store. A deployment is one
or more `databend-query` nodes (SQL frontend, planner, vectorised executor, storage access) backed by a
`databend-meta` cluster (Raft-replicated metadata: tenants, users, roles, grants, databases, tables,
stages, UDFs) and an object store (S3/GCS/Azure/... or a local `fs` backend) that holds table data in
the Fuse format (Parquet-like blocks plus snapshot/segment metadata).

Layout, by trust-relevant area (paths are relative to the repo root; everything is under `src/`):

- **Protocol servers** (`query/service/src/servers/`): `http/` (REST query API on `:8000`, sessions,
  stage upload, streaming load, catalog browsing, user/role management), `mysql/` (MySQL wire protocol
  on `:3307`, built on `opensrv-mysql`), `flight_sql/` (Arrow Flight SQL on `:8900`), `flight/` (the
  internal node-to-node Arrow Flight API on `:9091`), `admin/` (unauthenticated-by-design operator API on
  `:8080`: config, settings, processes, cluster, tenant tables, query dumps), `metrics/` (Prometheus).
- **Authentication and authorization**: `query/service/src/auth.rs`, `query/users/src/` (users,
  roles, grants, password/network policies, `jwt/` for JWKS-based auth, `visibility_checker.rs`),
  `query/service/src/sessions/session_privilege_mgr.rs`, and the per-plan privilege gate in
  `query/service/src/interpreters/access/privilege_access.rs`. Enterprise features in `query/ee/` and
  `query/ee_features/` add `data_mask`, `row_access_policy`, `storage_encryption`, `data_sharing`,
  `attach_table`.
- **SQL frontend**: `query/ast/` (hand-written nom parser), `query/sql/` (binder, optimizer, planner),
  `query/expression/`, `query/functions/` (all built-in scalar/aggregate/table functions, including
  `variant`/JSONB, string, regexp, geo, decimal), `query/script_udf_support/` (embedded JavaScript via
  `rquickjs`, WASM via `arrow-udf-runtime`; Python is behind the non-default `python-udf` feature).
- **External data ingress**: `query/storages/stage/` (`COPY INTO`, `INSERT ... FROM @stage`, streaming
  load, `infer_schema`), `query/formats/` (CSV/TSV/NDJSON/JSON decoders), `query/storages/parquet/`,
  `query/storages/orc/`, Avro under `storages/stage/src/avro_utils.rs`, plus external catalogs
  (`query/storages/iceberg/`, `paimon/`, Hive). `common/storage/` wraps OpenDAL and resolves
  user-supplied stage/location URIs and connection credentials; `common/compress/` handles
  decompression of staged files.
- **Table storage engine**: `query/storages/fuse/` (snapshots, segments, blocks, bloom/range indexes,
  time travel, vacuum), `query/storages/common/` (table_meta, cache, index), `common/column/`,
  `common/hashtable/`, `common/base/` (memory tracking, runtime).
- **Metadata service**: `meta/` (`service`, `binaries/meta`, `app`, `api`, `proto-conv`, `protos`).
  Query nodes talk to it over gRPC with a per-tenant client; `admin/` exposes an HTTP admin API.

## Where untrusted input enters

Treat these as attacker-controlled unless noted:

1. **SQL from an authenticated user over HTTP, MySQL or Flight SQL.** Databend is multi-tenant and
   RBAC-based: a user with low privileges in one tenant is the primary adversary. Any SQL (including
   table functions, `SET`, `EXPLAIN`, system tables, UDF calls, `PRESIGN`, `COPY`) must be checked
   against the caller's grants and must not touch another tenant's metadata or data.
2. **Protocol framing before authentication**: HTTP headers/body (`X-Databend-*` session headers,
   `Authorization`, login/refresh/logout handlers under `http/v1/session/`), the MySQL handshake and
   packet parser (`opensrv-mysql` integration in `servers/mysql/`), Flight SQL handshake and
   `FlightDescriptor` payloads.
3. **Files pulled from stages and external locations.** `COPY INTO`, streaming load (`PUT
   /v1/streaming_load`), `PUT /v1/upload_to_stage`, `INFER_SCHEMA`, and `SELECT ... FROM @stage` parse
   attacker-supplied Parquet, ORC, Avro, CSV/TSV, NDJSON and compressed (gzip/zstd/bz2/xz/snappy/deflate)
   payloads. A crafted file must not cause memory unsafety, unbounded allocation, hangs, or a panic that
   takes down the query node (one process serves many tenants).
4. **Stage / connection / external-table definitions**: URIs, endpoints, and credentials supplied in
   `CREATE STAGE`, `CREATE CONNECTION`, `COPY ... LOCATION`, `ATTACH TABLE`, external catalogs
   (Iceberg REST/Glue/HMS, Hive, Paimon). The query node will make outbound requests to what it is told.
5. **Fuse table metadata on object storage** (`snapshot`, `segment`, block metadata, index files). In
   shared-storage and `ATTACH TABLE` scenarios, that data may be written by a different tenant or a
   different trust domain. Decoders in `query/storages/common/table_meta` and `fuse/src/io` should not
   trust lengths/offsets.
6. **JWT tokens and JWKS responses** (`query/users/src/jwt/`): issuer/audience/`exp` handling, key
   selection, algorithm confusion, claim-to-user/tenant/role mapping, JWKS refresh behaviour.
7. **UDF bodies** (JavaScript/WASM; Python if enabled) and the arguments they receive. The UDF author is
   a database user, not an operator; the runtime must not let a UDF read the host filesystem,
   environment, or network, exhaust memory/CPU without limits, or escape the process.
8. **Metadata written to `databend-meta`** by one query node is read by all others: a value that
   deserialises badly (`meta/proto-conv`) can crash every node in the tenant.

Not attacker-controlled: the query/meta configuration files, CLI flags, the admin API (`:8080`) and
metrics endpoint, which are documented as operator-only and expected to be firewalled. Report bugs in
them only when they are reachable from a SQL user or when a documented default leaks across tenants.

## What matters most

In order of importance:

1. **Cross-tenant isolation.** Any read, write, DDL, or metadata listing that crosses a tenant boundary
   (tenant comes from the authenticated user, from `X-Databend-Tenant` in sandbox mode, or from the
   JWT), including via system tables, information_schema, `SHOW` commands, table functions, stages,
   shares, `ATTACH TABLE`, time travel, or the internal Flight API between nodes.
2. **Authentication bypass** on any handler: HTTP session tokens (`http/v1/session/token.rs`), refresh
   logic, MySQL auth plugins (`double_sha1_password`, `sha256_password`, `no_password`), Flight SQL
   handshake, JWT validation, network policy / password policy enforcement, `login_history`.
3. **Privilege escalation within a tenant.** `privilege_access.rs` is the gate: look for plans or table
   functions that skip it, ownership transfers, role grants that create cycles or reach `account_admin`,
   `GRANT ... ON *.*`, secondary roles, `EXECUTE AS`, default-role handling, system tables that leak
   other users' queries/parameters/credentials (`system.query_log`, `system.processes`,
   `system.stages`, `system.connections`, `system.user_functions`, `SHOW GRANTS`, `DESCRIBE STAGE`).
   Row access policies and data masking (EE) must be applied for every access path including
   `COPY INTO LOCATION`, `PRESIGN`, `STREAM`, `MERGE`, `UPDATE ... RETURNING`-style paths, subqueries,
   views, and materialized views.
4. **Credential and secret exposure**: stage/connection AK/SK, session tokens, JWT keys, or storage
   encryption keys appearing in error messages, `EXPLAIN`, `SHOW CREATE`, logs, query_log, or
   `admin` endpoints. `PRESIGN` must only presign objects the caller can already access.
5. **SSRF and outbound request control**: stage/connection URIs that point at cloud metadata endpoints
   (`169.254.169.254`), internal hostnames, `file://` or local `fs` paths that escape the configured
   data root, or that use the query node's own IAM/instance credentials for a user-supplied bucket.
6. **Memory safety and robustness against crafted files/metadata**. The `unsafe` density is highest in
   `query/expression`, `query/functions`, `common/column`, `common/hashtable`, `common/base`,
   `query/pipeline`, `query/storages/fuse` and `query/formats`. A panic in a worker thread is caught
   per query, but an abort, UB, infinite loop, or multi-GB allocation hits every tenant on the node.
7. **Durability and correctness of committed data**: lost or corrupted data after `INSERT`/`COPY`/
   `MERGE`/`DELETE`/`UPDATE`, `OPTIMIZE`, `VACUUM`, `RECLUSTER`, snapshot conflicts under concurrent
   writers, time travel returning data the caller should no longer see. A wrong result from a crafted
   query is a security bug for us when it bypasses a policy or privilege; otherwise it is a normal bug.
8. **Denial of service from SQL**: deep nesting in the parser or binder (stack overflow), pathological
   regexes, unbounded `GENERATE_SERIES`/`range`/`unnest`, settings a plain user can raise beyond
   configured limits (`max_memory_usage`, `max_threads`, `max_execute_time_in_seconds`), or
   `KILL` of another user's query.

## Out of scope / lower value

- `tests/`, `benchmark/`, `scripts/`, `docker/`, `tools/`, `website/` and anything that only runs in
  CI. Bugs only reachable from test harnesses, `--internal-enable-sandbox-tenant`'s **intended**
  behaviour (it exists so one test server can emulate many tenants; treat the sandbox header as trusted
  only when that flag is set), or `databend-sqllogictests` are not security issues.
- The `admin` API and metrics ports, config files and CLI flags (operator-controlled; see above).
- Features gated behind a license (`query/ee/`) still matter for isolation, but "feature works without
  a license" is a business issue, not a security one.
- Dependencies: report a vulnerable dependency only if Databend reaches the vulnerable code.
- Documented differences from MySQL/PostgreSQL/Snowflake SQL semantics are not bugs.
- Timing side channels, except in password/token comparison.
- `databend-metactl`, `databend-metabench`, `databend-metaverifier`, `table-meta-inspector`,
  `bendsave` (operator CLIs run against trusted local data).

## How we rate severity

- Cross-tenant read or write of data or metadata by an authenticated low-privilege user: **critical**.
- Authentication bypass on HTTP, MySQL, Flight SQL, or JWT: **critical**.
- Privilege escalation to `account_admin`, ownership takeover, or RBAC bypass that reads/writes tables
  the caller has no grant on: **high**; **critical** if it is trivial from a default-created user.
- Row access policy or data mask bypass: **high**.
- Credential/secret disclosure (stage keys, connection secrets, session tokens, JWT private keys):
  **high**; **critical** if cross-tenant.
- SSRF reaching cloud metadata or internal services with the node's credentials: **high**.
- UDF sandbox escape (filesystem/network/env access, or arbitrary code outside the JS/WASM runtime):
  **critical**; UDF resource exhaustion that an operator setting cannot bound: **medium**.
- Memory corruption (OOB write, UAF, type confusion) reachable from SQL, a staged file, or table
  metadata: **critical** if attacker data reaches the corrupted memory, otherwise **high**. OOB read /
  info leak from a crafted file: **high**.
- Process abort, UB-free infinite loop, stack overflow, or unbounded allocation from a crafted file or
  from short ordinary-looking SQL: **medium** (multi-tenant process); from SQL that is itself
  pathological and long: **low**.
- Silent loss or corruption of committed data under supported operations: **high**; **critical** if
  easy to trigger in normal use.
- Issues in the operator-only admin API, metrics, or config parsing: **low** unless reachable from a
  SQL user or combined with another finding.
- Anything requiring a malicious operator, a malicious config file, or a malicious UDF runtime binary:
  **informational**.

## How to exercise it

Everything is built in debug mode under `/src/target/debug`, every crate is fetched, and
`CARGO_NET_OFFLINE=true` is set, so nothing below needs the network.

- **Start a standalone server** (meta + query, local `fs` storage under `./.databend`):
  `bash ./scripts/ci/deploy/databend-query-standalone.sh`. This passes
  `--internal-enable-sandbox-tenant`, which makes the `X-Databend-Tenant` header trusted; drop that flag
  (run `target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml` yourself)
  when you want production tenant handling. Ports: HTTP `8000`, MySQL `3307`, Flight SQL `8900`,
  admin `8080`, meta gRPC `9191`. Default user `root` with `no_password` from `127.0.0.1`
  (`databend-query-node-1.toml`, `[[query.users]]`); create more users with `CREATE USER` to test RBAC.
- **HTTP**: `curl -u root: -H 'Content-Type: application/json' -d '{"sql":"select 1"}'
  http://127.0.0.1:8000/v1/query`. Paginate with the returned `next_uri`. Session handlers are under
  `/v1/session/{login,refresh,logout}`, stage upload is `PUT /v1/upload_to_stage`, streaming load is
  `PUT /v1/streaming_load`.
- **MySQL**: `mysql -h127.0.0.1 -P3307 -uroot`.
- **Python**: `databend-driver` and `mysql-connector-python` are installed.
- **SQL logic tests**: `target/debug/databend-sqllogictests --handlers http,mysql --run_dir base/05_ddl
  --enable_sandbox` (suites live in `tests/sqllogictests/suites/`; `--skip_dir management,ee,temp_table`
  matches CI's standalone run). Stateless Python tests: `cd tests && ./databend-test --mode standalone
  --run-dir 0_stateless`.
- **Unit tests**: `cargo nextest run -p <crate>` (`RUST_TEST_THREADS=2` is preset). Test binaries for
  the users, formats, io, column, hashtable, ast, sql, stage and parquet crates were pre-compiled; others
  compile on demand (slow on 2 vCPUs, so prefer `-p` to `--workspace`). Rebuilding `databend-query`
  after a patch takes a few minutes with `CARGO_BUILD_JOBS=2`; do not raise it on the 8 GB VM.
- **Crafted files**: `CREATE STAGE s;` then `PUT` the file to `/v1/upload_to_stage` with headers
  `x-databend-stage-name: s` and `x-databend-relative-path: dir/`, or point a stage at a local directory:
  `CREATE STAGE s URL='fs:///tmp/data/'` (the `fs` scheme is accepted by
  `query/sql/src/planner/binder/location.rs`; note that letting a SQL user read arbitrary host paths
  this way is itself worth examining).
- Logs: `.databend/logs_1/` (query) and `.databend/logs1/` (meta).

## How we want reports and patches

- A reproducer we can run against the standalone server: a SQL script, `curl` or `mysql` session
  transcript, a staged file plus the `COPY`/`SELECT` that triggers the bug, or a Rust test. Say which
  commit you built and which user/role you ran as.
- For isolation and RBAC findings, show both the grant state (`SHOW GRANTS FOR ...`) and the access
  that should have been denied.
- For memory-safety findings, show the failing invariant (panic/abort output with `RUST_BACKTRACE=1`,
  sanitizer output, or the wrong bytes).
- Patches should be minimal, follow the surrounding style, keep existing comments, and include a
  regression test: a `.test` file under `tests/sqllogictests/suites/` for SQL-visible behaviour, or a
  unit test in the affected crate. `cargo fmt` and `cargo clippy -D warnings` run clean in CI.

## Leave alone

- `expect`/`unwrap`/`assert` on states the report cannot reach from any input listed above.
- Clippy-level findings, unused `unsafe`, theoretical issues with no reachable trigger.
- Performance regressions and planner choices, unless they are an algorithmic DoS from short SQL.
- Behaviour behind experimental settings (`enable_experimental_*`) beyond isolation and memory safety.
