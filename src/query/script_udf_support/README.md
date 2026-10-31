# Script UDF execution

JavaScript and WebAssembly retain their in-process execution paths. With the
`python-udf` feature, **Python script UDFs** (scalar and aggregate) execute in
fresh child processes of the query executable. The hidden worker entrypoint runs
before service initialization; workers do not start query servers or load
service configuration.

Responsibilities are split between this crate and the `datafuse-extras/arrow-udf`
fork:

- `arrow-udf-python-worker` (fork): worker launch, the IPC protocol, process
  lifecycle (kill/reap of the worker's process group), the environment
  allowlist and pre-exec hardening, and the Landlock/seccomp sandbox.
- `arrow-udf-runtime` (fork): the JavaScript engine, including its
  operation-wide execution deadline and memory limit.
- This crate: configuration and defaults, query admission, quotas and pooling
  (`python_worker.rs`, `python_worker_pool.rs`), pipeline lifecycle, aggregate
  state ownership, dependency installation and the worker entrypoint, which
  stops jemalloc background threads before the sandbox is entered.

## Isolation and scheduling

- A Python runtime belongs to one local query execution and one UDF definition.
  It is not cached by function name across SQL executions.
- Each runtime owns a worker pool. Parallel pipeline transforms can check out
  different workers, bounded by the query's current `max_threads` and node/tenant
  capacities. Pools grow only when concurrent batches need them: a scalar query
  does not start `max_threads` processes. Workers are reused within the execution;
  no sharing of Python globals between workers is promised.
- Pipeline construction registers all local Python runtimes with the shared
  query context. Before starting any worker, the query reserves one minimum
  worker per runtime together, tenant quota first and node quota second. Every
  admitted operator can therefore make progress even when expansion is blocked.
- Additional workers use nonblocking quota acquisition. If extra capacity is
  unavailable, batches wait for an existing worker, not for incremental quota.
  Idle extras can be retired to serve another operator or query; busy extras
  yield on completion when minimum admissions are waiting. Reclaimed workers
  are killed and reaped, never handed to another SQL execution.
- Idle extras are also automatically retired after five minutes without a batch,
  even when there is no capacity pressure or subsequent request. Idle time starts
  when a successful request returns its worker to the pool, and resets on reuse.
  Each pool has at most one deadline-driven timer while idle extras exist; the
  timer does not keep the runtime alive. Busy workers are never idle-expired.
  The minimum worker per runtime is kept for progress; query completion still
  terminates all workers immediately, without waiting for idle expiry.
- The admission queue is bounded. Full queues, queue timeouts, excessive query
  budgets, and attempts to add runtimes after admission return errors. They never
  cause unbounded process creation or an impossible resource wait. A repeated
  occurrence of a function in a separately built physical operator currently
  counts as another runtime.
- Workers are started lazily using exec, not fork-without-exec. A worker can
  process multiple Arrow batches within the same query, but is never handed to a
  different query after running user code.
- Completion of the last registered local pipeline, or query cancellation,
  terminates workers, including their process groups, and reaps them
  asynchronously on the service runtime. Finishing one fragment does not kill
  workers still needed by another fragment. Quota and private
  directories remain alive until the child has actually exited. Starting and
  exiting workers count against the budget.
- A failed, cancelled, or timed-out request poisons its runtime. Batches are not
  retried automatically: a Python function may already have external side effects.

The execution timeout applies to each worker request, including interpreter
startup on the first request. Query cancellation also interrupts admission waits.
The normal query timeout still controls total SQL execution time.

## Configuration

All values are under `[query]` in the query service configuration:

```toml
python_udf_max_workers = 0
python_udf_max_workers_per_tenant = 0
python_udf_max_workers_per_query = 0
python_udf_max_queued_queries = 128
python_udf_queue_timeout_secs = 60
python_udf_execution_timeout_secs = 300
python_udf_worker_idle_timeout_secs = 300
```

Zero selects automatic sizing:

- Node: configured `query.num_cpus`, or the process's available CPU parallelism
  when `num_cpus=0`. This respects CPU allocation rather than assuming host cores.
- Tenant: inherit the node worker capacity.
- Query: if `M` runtimes are registered and effective parallelism is `P`, allow
  at most `M + P - 1` resident workers. `P` follows the SQL's `max_threads`, bounded
  by node/tenant capacity. Only `M` minimum slots are reserved up front; the rest
  are acquired on demand. A shared execution semaphore limits all operators
  together to `P` in-flight calls, not `P` calls per operator.

An explicit nonzero query limit caps total resident workers, including minima,
starting workers, and extras; it also caps execution parallelism. Nonzero limits
must satisfy `query <= tenant <= node`. Queue capacity and timeouts must be
positive; the idle timeout defaults to 300 seconds (five minutes). It is separate
from admission and execution timeouts and only shrinks idle extra capacity.
These are local-node limits, not warehouse-wide limits. A minimum
budget that cannot fit fails during pipeline construction rather than waiting
forever. Runtime count is not limited to `max_threads`: multiple UDF operators
can still execute sequentially when `max_threads=1`.

For example, one UDF on a 128-CPU allocation with `max_threads=64` can lazily use
up to 64 workers. Multiple queries share the same 128-worker node ceiling; they
cannot each create an independent 128-worker allocation. CPU-based sizing does
not imply a safe memory budget: use explicit smaller capacities for
memory-heavy packages until OS-enforced worker memory limits are available.

## IPC and dependency files

A private Unix-domain socket carries length-delimited initialization metadata and
Arrow IPC streams. Each message has a 256 MiB limit; reduce the SQL block size for
larger payloads. User stdout/stderr are discarded, not used as protocol channels;
Python exceptions are returned through the response protocol.

Dependency installation and stage artifact archives are cached. Every Python
runtime receives a private, unexecuted template, and each worker gets a separate
copy, never a shared writable directory or hardlinks. Dependency copying runs on
the blocking runtime and retains startup quota even if its awaiting task is
cancelled. Workers without dependencies also receive a private working directory.
Python state such as `sys.path`, `sys.modules`, `builtins`, OS environment
variables, and working-directory changes cannot leak
through a shared interpreter into another query.

## Sandbox

Every Python worker enters an unprivileged OS sandbox (implemented by
`arrow-udf-python-worker`) after the interpreter has started and before any user
code runs. The sandbox itself cannot be disabled: if it cannot be entered, the
worker reports
`Python UDF sandbox is unavailable, refusing to run user code: ...` and the
query fails. Requirements: Linux 5.13+ with `landlock` in the active LSM list
(`/sys/kernel/security/lsm`), seccomp, x86_64 or aarch64, and a container
seccomp profile that permits the `landlock_*` syscalls.

macOS has neither Landlock nor seccomp and is supported for local development
only: workers there run **without** the sandbox (environment allowlist, private
directory and `RLIMIT_CORE=0` still apply) and the query node logs a warning
when the worker manager starts. Do not serve untrusted UDFs from macOS. Other
non-Linux platforms refuse to run Python UDFs.

### Integration-bound HTTP(S) access

Local Python scalar and aggregate scripts can bind the existing SQL-managed
`NETWORK RULE` / `EXTERNAL ACCESS INTEGRATION` objects used by remote UDFs.
The operator must explicitly opt in on every executing query node (default is
`false`); this is not a tenant SQL setting and never enables native sockets:

```toml
[query]
enable_python_udf_external_access = true
# Optional hostname ceiling. Non-public destinations additionally require their
# exact IP literal here; trusting a hostname alone never bypasses the IP check.
udf_server_allow_list = ['https://api.example.com']
```

Without this opt-in, CREATE/ALTER with Python external-access bindings and worker
startup with existing bindings fail closed. Missing query config also fails closed.
`enable_udf_sandbox = true` continues to select the external scalar sandbox route;
CREATE/ALTER rejects external-access bindings on that unsupported route rather
than changing routing or accepting a function that cannot run.

```sql
CREATE NETWORK RULE events_rule TYPE = HOST_PORT MODE = EGRESS
    VALUE_LIST = ('api.example.com:443');
CREATE EXTERNAL ACCESS INTEGRATION events_access
    ALLOWED_NETWORK_RULES = (events_rule) ENABLED = TRUE;
GRANT USAGE ON INTEGRATION events_access TO ROLE udf_developer;

CREATE FUNCTION fetch_events(STRING) RETURNS STRING LANGUAGE python
PACKAGES = ('requests') HANDLER = 'handler' AS $$
import requests

def handler(key):
    response = requests.get('https://api.example.com/events', params={'key': key}, timeout=10)
    response.raise_for_status()
    return response.text
$$ EXTERNAL_ACCESS_INTEGRATIONS = (events_access);
```

Use the same suffix for `ALTER FUNCTION` or Python aggregate definitions. Existing
CREATE/ALTER privileges are unchanged; binding additionally requires USAGE on
every integration. Definitions persist the binding, and both stored metadata and
distributed plans reject older readers/executors that cannot enforce it.
Invocation follows existing UDF privileges, not integration USAGE. Revoking
USAGE prevents future bindings/ALTERs; it does **not** revoke networking from an
already-bound function. Disable/drop an integration or change/drop its network
rules to revoke egress. All named integrations must remain present and enabled;
their rules form a union, matching exact canonical HOST_PORT values. An omitted
rule port means 443. A nonempty operator `udf_server_allow_list` remains the same
hostname-only ceiling used for remote UDFs; a tenant rule must authorize the port.
`enable_udf_server` and `udf_server_allow_insecure` control remote UDF transport,
not the separate Python operator opt-in.

The worker still cannot create Internet sockets. Proxy-aware synchronous clients
such as `urllib.request` and `requests` use a private host-controlled transport;
the host authorizes each HTTP target or HTTPS CONNECT destination and performs
DNS/connect itself. Cross-destination redirects require a newly authorized HTTP
request or CONNECT tunnel; Host headers cannot change the checked destination.
HTTP Upgrade and arbitrary TCP/UDP/native-socket clients are unsupported. HTTPS
is end-to-end: the proxy does not decrypt TLS or implement application-level
hostname/SNI filtering inside a CONNECT tunnel. CONNECT approval grants an
opaque byte stream to the approved endpoint, not a guarantee that its payload is
TLS or HTTP. The host validates every actual DNS result before connecting, then
connects only to those checked addresses (no second hostname lookup). By default,
only globally routable unicast IPs are accepted: loopback, link-local/metadata,
unspecified, private, shared, reserved and multicast addresses are rejected for
both IPv4 and IPv6, including IPv4-mapped IPv6. Deprecated IPv4-compatible,
site-local and well-known IPv6 translation ranges also require an IP exception.
Mixed public/non-public DNS answers fail closed. An operator may explicitly permit a non-public IP by adding
that exact IP literal to `udf_server_allow_list`; an allowed hostname resolving
to that IP is insufficient. This IP exception retains the list's hostname-only
semantics, so the operator entry covers all ports but a tenant HOST_PORT rule
must still authorize the actual port. Tenant admins cannot grant IP exceptions.

An empty ceiling delegates public-host selection to tenant rules only after
operator opt-in; it never removes the non-public-IP guard. In multi-tenant
Cloud, the operator controls opt-in and IP exceptions, not the tenant admin.
Use only trusted endpoints: authorization of a host/IP does not constrain its
application behavior or prevent an explicitly approved endpoint acting as a
relay. Endpoint rules are not application-payload or TLS SNI enforcement.

Each active HTTP connection and CONNECT tunnel rechecks policy every second,
including while idle between batches. A failed/denied check closes the connection;
each check has a five-second deadline. Revocation follows the existing remote-UDF
asynchronous meta-watch propagation, plus this check interval/deadline; there is
no instantaneous cross-node revocation guarantee. Workers need not restart to
see rule changes. Query cancellation/completion closes their proxy streams.
Connections have a five-minute maximum lifetime and at most 32 concurrent host
streams per worker. Worker-to-proxy request headers have a five-second read
deadline; DNS plus TCP connect has a separate five-second deadline. Upstream
response headers have no separate deadline and are bounded by the overall
five-minute lifetime (and client/query cancellation). Plain HTTP requests
and responses are streamed with a 16 MiB body limit; opaque CONNECT tunnels have
no application-body inspection. Large downloads may require multiple requests.

Without bindings, workers cannot use the network. There is no configuration,
CLI or SQL setting that enables unrestricted Python worker networking. Bound
workers also cannot create Internet sockets: only approved HTTP/CONNECT targets
are reachable through the host proxy. This mode requires the enforced Linux
sandbox. It does not change execution routing: unsupported/nonlocal script
execution routes reject bindings rather than drop the policy. Package installation
remains a separate host-side operation, not traffic governed by these per-function
rules.

Landlock filesystem restrictions and seccomp process restrictions remain
mandatory. Proxy-bound workers may read public CA certificate paths (including
configured `SSL_CERT_FILE`/`SSL_CERT_DIR`) for end-to-end HTTPS verification, but
cannot create IPv4/IPv6 or Unix sockets to local services.

Uncaught Python exceptions, including network permission errors, fail SQL
execution; they must not be treated as successful NULL results. Python code
that catches an exception and deliberately returns NULL or an `{"error": ...}`
JSON value still returns a normal value. To fail an ingestion task, let the
exception propagate or explicitly validate the returned business status.

Landlock (filesystem; default policy):

- read only: the stdlib, `lib-dynload` and site-packages of the environment and
  its base installation, every `sys.path` and `LD_LIBRARY_PATH` entry (except
  `/`), system library directories (`/lib*`, `/usr/lib*`, `/usr/local/lib`),
  `/usr/share/zoneinfo`, `/etc/localtime`, `/etc/ld.so.cache`, `/dev/zero`,
  `/dev/random`, `/dev/urandom`, `/proc/cpuinfo`, `/sys/devices/system/cpu` and
  the worker's own `/proc/<pid>`;
- read/write: `/dev/null` and the worker's private directory, which is its cwd,
  `TMPDIR` and the location of dependencies and stage imports;
- everything else is denied, including query configuration and data
  directories, `/proc/<query pid>/environ` and other processes' `/proc` entries.
  Path lookup is not restricted: `stat` still reveals whether a path exists.
- on ABI 4+ TCP bind/connect and on ABI 6+ signals and abstract Unix sockets
  outside the worker are denied by Landlock as well, including proxy-bound workers.

seccomp (deny list, EPERM unless noted):

- `socket` (EACCES): no IPv4/IPv6 sockets and no Unix sockets to other local
  services; `bind`/`listen` remain denied. The IPC connection and private proxy
  transport are created before the filter;
- `execve`/`execveat`: `subprocess`/`os.system` cannot start programs;
- `fork`/`vfork` and `clone` without `CLONE_THREAD`: no child processes, so
  user code cannot escape the per-worker process accounting or fork-bomb;
- `kill` only to the worker itself or its process group, `tgkill` and
  `rt_*sigqueueinfo` only to its own threads, no `tkill`/`pidfd_send_signal`;
- `ptrace`, `process_vm_*`, `pidfd_getfd`, `io_uring_*`, `bpf`,
  `perf_event_open`, `userfaultfd`, keyrings, namespaces (`unshare`, `setns`,
  `clone` with `CLONE_NEW*`; `clone3` returns ENOSYS so libc falls back to
  `clone`), mounts, `chroot`, kernel modules, `kexec`, `reboot`, swap, handle-based
  opens;
- path-based `chmod`/`chown`/`*xattr`, which Landlock does not control
  (`shutil.copy`/`copystat` fail; `shutil.copyfile` works); `truncate` too on
  Landlock ABI < 3;
- x32 syscalls (ENOSYS) and foreign architectures (process killed).

The sandbox is entered single-threaded (Landlock only confines the calling
thread and its future children), so the worker first stops jemalloc's background
purge threads (`background_thread:true` in the query binary).

Known limits: workers still run as the query service user, so the kernel, not a
separate uid, is the boundary; memory and CPU are not bounded (see below);
`/dev/shm` is not available, so `multiprocessing` locks and managers fail.

Additional hardening:

- Workers start with a cleared environment. Only an allowlist is inherited:
  `PATH`, `HOME`, locale (`LANG`, `LANGUAGE`, `LC_*`, `TZ`), library and
  interpreter lookup (`LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`, `PYTHONHOME`,
  `PYTHONPATH`, `PYTHONIOENCODING`, `PYTHONUTF8`, `PYTHONHASHSEED`), CA bundles
  (`SSL_CERT_FILE`, `SSL_CERT_DIR`) and BLAS thread counts. `TMPDIR` is set to
  the private directory.
- Workers run with `no_new_privs` and `RLIMIT_CORE=0`.
- `pip install` runs in the query node, not in the sandbox. It receives only
  `name[extras]<version specifiers>` requirements, after `--`, so requirements
  can never become pip options, URLs, direct references, local paths or
  environment markers. pip runs with the same environment allowlist plus `PIP_*`,
  proxy, CA-bundle and XDG variables, so operator-configured mirrors keep
  working. With `python_udf_pip_only_binary = true` (default) only wheels are
  installed: source distributions would run build scripts on the query node. Set
  it to `false` for sdist-only packages.

The process limit bounds manager-created workers. Process-group cleanup is best
effort and is not a substitute for cgroups.

`RESTRICTED_PYTHON_CODE` is an in-interpreter blacklist that is trivially
bypassed (for example through `io.open` or `ctypes`); the OS sandbox above is the
boundary.

Python worker memory is not charged to the query process allocator. OS-enforced
per-worker/aggregate memory limits and metrics need separate resource-controller
integration before treating this as an OOM containment boundary. In particular,
a process-count limit alone does not bound total memory.

## Python aggregate UDFs

Python aggregate script UDFs (`udaf_script.rs`) run in the same worker pool,
sandbox and quotas as scalar ones: a worker started for an aggregate loads its
`create_state`/`accumulate`/`merge`/`finish` functions instead of a handler.

- States stay in the query process (as Arrow arrays, serialized for spilling and
  exchange exactly as before) and travel with every request, so workers are
  stateless: any pooled worker serves any request, and a killed worker loses no
  aggregation state.
- Updated group states are copied out of the worker response, so a state never
  keeps the whole response of its block alive.
- Requests are batched per block: `accumulate_keys` sends the distinct states of
  a block, a row-to-state index and the arguments once; the worker applies rows
  to their state in row order. `merge_serialized` does the same for partial
  states. Final results (`merge_result`) are still one request per group, because
  the aggregate API finalizes states one at a time.
- The initial state is created by the first request, not while the pipeline is
  built: the first worker request admits the query, after which no other Python
  runtime of the query may register.
- Aggregates do not install `packages`/`imports` (unchanged).

They are still rejected (at `CREATE FUNCTION`, at binding, and on the executing
node) when `enable_udf_sandbox = true`, which routes scalar Python UDFs to an
external sandbox service that has no aggregate API; otherwise they follow
`enable_udf_python_script`.

## JavaScript and WebAssembly limits

JavaScript (QuickJS) and WebAssembly (wasmtime + WASI) have no file, network,
process or environment access: no WASI directories, sockets or variables are
provided and the JavaScript `fetch` feature is not built. Their remaining risk is
resource exhaustion inside the query process, bounded by:

```toml
udf_script_memory_limit_mb = 1024        # JS heap per runtime / WASM linear memory per instance, 0 = unlimited
udf_script_execution_timeout_secs = 300  # JS only: per call and for top-level module code, 0 = unlimited
```

The JavaScript deadline is enforced by `arrow-udf-runtime` and spans a whole
operation (`call`, `create_state`, `accumulate`, `merge`, `finish`): argument
conversion, the user function, continuations of async functions and Arrow
conversion of the result, which can run user getters and `toJSON`. It is set
before functions are added, so top-level module code is bounded too.

WebAssembly has no execution timeout yet: `arrow-udf-runtime` creates the
wasmtime `Store` internally, so epoch/fuel deadlines need an upstream change.
Neither engine observes query cancellation during a running call.

## Validation

```sh
# Point to a Python >= 3.12 installation. For non-system installations also add
# its lib directory to LD_LIBRARY_PATH when running tests or binaries.
PYO3_PYTHON=python3.12 cargo test -p databend-query-script-udf-support --features python-udf
PYO3_PYTHON=python3.12 cargo clippy -p databend-query-script-udf-support --features python-udf --all-targets -- -D warnings
```

`tests/python_worker_process.rs` uses a custom test main so child processes execute
the same worker protocol as the query binary. It requires no pip,
metastore, license, or network access. It covers concurrent same-definition SQL
isolation, lazy pools at parallelism 1/4/8, reuse and reclamation within/between
queries, minimum admission, process failure, timeout, multi-worker cancellation,
and quota recovery, and worker hardening (environment allowlist, `no_new_privs`,
no core dumps). It also uses a shortened idle timeout to verify that busy
workers survive, idle extra processes exit and are reaped without new requests,
the base worker stays reusable, and the pool can expand again. Unit tests use
paused Tokio time to cover the default five-minute boundary, reuse resetting idle
age, timer cancellation, and runtime lifetime; they also simulate 64/128-CPU
capacities without starting that many processes. SQL service coverage lives in
`tests/suites/5_ee/08_udf_python/08_0002_process_isolation.sh` and
`08_0003_worker_pool.sh`.

## Performance comparison

See [PERFORMANCE.md](PERFORMANCE.md) for the measured main/isolated comparison,
including the substantial fresh-worker startup cost and resource tradeoffs.

Build `main` and this branch with identical features, toolchain, profile, and
Python installation. Run servers sequentially with identical configuration and
no unrelated workload. Restart each server before measuring `cold_first`.

```sh
python3 scripts/benchmark/query/python_udf_isolation.py \
  --endpoint http://127.0.0.1:8000 --label main --revision MAIN_SHA \
  --pid QUERY_PID --output /tmp/python-udf-main.json
python3 scripts/benchmark/query/python_udf_isolation.py \
  --endpoint http://127.0.0.1:8000 --label isolated --revision BRANCH_SHA \
  --pid QUERY_PID --output /tmp/python-udf-isolated.json
python3 scripts/benchmark/query/python_udf_isolation.py \
  --compare /tmp/python-udf-main.json /tmp/python-udf-isolated.json
```

For a single-query scaling comparison, use `--workload gcd-scaling --threads N`
for `N=1,4,8` (and a sufficiently large explicit node capacity), optionally with
`--gcd-rows` to adjust input size. This uses one concurrent SQL query, not eight
independent queries, and validates results outside the timed region.

For actual greatest-common-divisor workloads, pass `--workload gcd` to each
measurement command. This runs a two-argument Euclidean Python UDF and a
`math.gcd` UDF, with both arguments varying per row. Every aggregate result is
checked against an independent standard-library `math.gcd` calculation outside
the timed SQL region. The report includes exact SQL, UDF code, and expected sums.

The standard-library-only driver records raw wall-time samples, medians, p95,
throughput, query outputs, and optional process/RSS samples. Workloads cover first
Python execution, scalar queries, small and large batches, CPU-heavy Python, and
8 concurrent queries. Warm-server measurements still include fresh worker startup
on the isolated branch. Summed RSS double-counts shared pages and 20 ms sampling
can miss short-lived workers; do not report it as unique physical memory.
SQL end-to-end timings do not independently attribute startup versus IPC costs.

The worker also watches IPC peer closure on a non-Python thread. If the query
node dies without running cleanup, a busy worker terminates rather than remaining
orphaned indefinitely. The process test includes killing a supervisor while its
worker is inside a long-running Python call.
