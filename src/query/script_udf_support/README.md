# Script UDF execution

JavaScript and WebAssembly retain their in-process execution paths. With the
`python-udf` feature, **scalar Python script UDFs** execute in fresh child processes
of the query executable. The hidden worker entrypoint runs before service
initialization; workers do not start query servers or load service configuration.

## Isolation and scheduling

- A Python runtime belongs to one local query execution and one UDF definition.
  It is not cached by function name across SQL executions.
- Each runtime has one worker. Pipeline transforms share that worker through an
  asynchronous mutex, rather than spawning one process per pipeline or batch.
  Python parallelism is deliberately independent of `max_threads` in this first
  implementation. No sharing of Python globals between parallel workers is
  promised.
- Pipeline construction registers all local Python runtimes with the shared
  query context. Before starting any worker, the query asynchronously reserves
  its entire worker budget, tenant quota first and node quota second. This avoids
  holding a worker while waiting for a second worker needed by the same query.
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
python_udf_max_workers = 16
python_udf_max_workers_per_tenant = 8
python_udf_max_workers_per_query = 4
python_udf_max_queued_queries = 128
python_udf_queue_timeout_secs = 60
python_udf_execution_timeout_secs = 300
```

Require `0 < query <= tenant <= node`, positive queue capacity and timeouts. These
are local-node limits, not warehouse-wide limits. Query budgets larger than the
configured limit fail during pipeline construction. Set the tenant and query
limits explicitly when lowering the node limit below their defaults.

## IPC and dependency files

A private Unix-domain socket carries length-delimited initialization metadata and
Arrow IPC streams. Each message has a 256 MiB limit; reduce the SQL block size for
larger payloads. User stdout/stderr are discarded, not used as protocol channels;
Python exceptions are returned through the response protocol.

Dependency installation and stage artifact archives are cached. Every Python
runtime receives a separate extraction of the cached archive, never the shared
live directory or writable hardlinks. Workers without dependencies also receive
a private working directory. Python state such as `sys.path`, `sys.modules`,
`builtins`, OS environment variables, and working-directory changes cannot leak
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
exactly the same worker entrypoint as the query binary. It requires no pip,
metastore, license, or network access. It covers concurrent same-definition SQL
isolation, reuse within a query, whole-query admission, process failure, timeout,
cancellation, and quota recovery. SQL service coverage lives in
`tests/suites/5_ee/08_udf_python/08_0002_process_isolation.sh`.

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
