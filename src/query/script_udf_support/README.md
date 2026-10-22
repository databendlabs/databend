# Script UDF execution

JavaScript and WebAssembly retain their in-process execution paths. With the
`python-udf` feature, **scalar Python script UDFs** execute in fresh child processes
of the query executable. The hidden worker entrypoint runs before service
initialization; workers do not start query servers or load service configuration.

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

## Boundaries

This change does **not** provide a security sandbox. Workers run as the query
service user; filesystem/network access and process creation remain subject to
existing OS permissions. The process limit bounds manager-created workers, not
arbitrary descendants created by user code. Process-group cleanup is best effort
and is not a substitute for cgroups or containers.

Python worker memory is not charged to the query process allocator. OS-enforced
per-worker/aggregate memory limits and metrics need separate resource-controller
integration before treating this as an OOM containment boundary. In particular,
a process-count limit alone does not bound total memory.

Python aggregate script UDFs (`udaf_script.rs`) have a separate state/merge API and
are **not migrated** here; their existing in-process behavior is unchanged. The
isolation guarantees in this document apply only to scalar Python script UDFs.

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
and quota recovery. It also uses a shortened idle timeout to verify that busy
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
