# Databend Meta Service Benchmarks

Benchmark entry points for `databend-meta`. Run every command below from the
repository root, because the scripts resolve binaries as `./target/<profile>/...`
and configs as repository-relative paths.

**Recommended: `meta-cluster-bench.py`.** Use it for any number you intend to
record or compare against another build. It reports latency percentiles next to
throughput and gives each node its own `databend-meta` binary, which is what
makes comparing two builds in one run possible. Give it an exported meta data
file with `--seed-file` so the workload measures against real state; without
one the cluster starts empty and the report labels the run "unseeded".

| Entry point | Cluster it starts | Use it for |
| --- | --- | --- |
| `meta-cluster-bench.py` | one node per `--node-bin` entry, optionally seeded | comparing two `databend-meta` builds, or replaying real data |
| `run_lua_benchmark.py` | 1 node by default, no seed data | driving load through `databend-metactl lua` instead of the gRPC bench client |

Both Python scripts print their full option list with `--help`. `cluster.py` is
not an entry point; they import it to start their cluster.

## meta-cluster-bench.py

Compare two builds in one run, one version on nodes 1 and 2 against another on
node 3. `--node-bin` takes one build directory per node, and the cluster is as
large as that list:

```bash
scripts/benchmark/meta/meta-cluster-bench.py \
  --node-bin ~/bin/v1.2.819-nightly ~/bin/v1.2.819-nightly ~/bin/v1.2.908-nightly \
  --node-label 819 819 908 \
  --seed-file ~/data/backup.data
```

The workload drives node 1 after making it leader, so the run above measures
the 819 build; `--leader 3` puts the 908 build in charge without reordering
`--node-bin`.

Measure the working tree's own build under a heavier load. When every node runs
the same build, `--node-count` sizes the cluster instead of repeating the
directory:

```bash
scripts/benchmark/meta/meta-cluster-bench.py \
  --node-bin ./target/debug --node-count 3 \
  --seed-file ~/data/backup.data \
  --workload-clients 16 --workload-ops 100000
```

The report prints throughput split into success and error rates, latency
average, maximum, p50, p90, p95, and p99, and the metabench histogram. Each
node line carries the version string the running binary reported, so a wrong
`--node-bin` shows up in the report rather than in the numbers. The script
exits non-zero when the run produced no usable measurement.

`--repeat N` runs the workload N times in one cluster session, prints every
run's qps with their median, and fills the table from the median run; use it
for any number meant to support an "A is faster than B" claim, since one run's
qps moves a few percent between runs. Successive runs write onto the same
cluster state, exactly like successive invocations do. `--workload-run-secs`
caps each run by wall clock through metabench's `--run-secs`.

`make meta-bench` builds release binaries and runs a quick unseeded pass
through this script: 3 nodes off `./target/release`, 10 clients x 1000 ops.

## run_lua_benchmark.py

```bash
scripts/benchmark/meta/run_lua_benchmark.py \
  scripts/benchmark/meta/lua/write_heavy_upsert_get.lua
```

The positional argument is the Lua workload. `write_heavy_upsert_get.lua` is
the one shipped here: 64 workers upserting and 4 workers reading keys those
writers are producing, 10,000 operations each. Edit `UPSERT_WORKERS`,
`GET_WORKERS`, and `OPERATIONS_PER_WORKER` at the top of the file to change the
shape. It reports per-second upsert and get rates, success and error counts,
and total duration.

A workload script reads the cluster's gRPC address from the Lua global
`GRPC_ADDR`, which the runner sets before loading the script. Open a new
workload with the fallback line below, so it also runs by hand against a meta
service you started yourself:

```lua
local GRPC_ADDR = GRPC_ADDR or "127.0.0.1:9191"
```

One node unless a `--config` asks for more, in which case all of them start and
the workload runs against whichever one is leader — that is how a Lua workload
measures against real raft replication instead of a lone local node:

```bash
scripts/benchmark/meta/run_lua_benchmark.py \
  scripts/benchmark/meta/lua/write_heavy_upsert_get.lua \
  --config three-nodes.toml
```

`--meta-bin` alongside such a config renames the build every node runs; the
config still decides how many nodes there are.

## Describing the cluster in a file

Both Python scripts take `--config FILE`, which sets the cluster up in TOML
instead of on the command line. Every command-line option overrides what the
file sets, so one checked-in config can serve a family of runs:

```toml
work_dir = "./.databend"
port_base = 28101
reset_work_dir = false
seed_file = "~/data/backup.data"

[raft_config]
heartbeat_interval = 500

[[nodes]]
bin = "~/bin/v1.2.819-nightly"
label = "819"
count = 2

[[nodes]]
bin = "~/bin/v1.2.908-nightly"
label = "908"
```

A `[[nodes]]` table describes a **group** of identical nodes, not a single
node, so a cluster of any size needs no enumeration. `count` is how many nodes
the group contributes and defaults to 1; the groups are laid out in order, so
the config above gives node1 and node2 the 819 build and node3 the 908 build. A
uniform cluster is therefore one table:

```toml
[[nodes]]
bin = "./target/debug"
count = 5
```

`bin` is the group's build directory and `label` names that build in the
report, defaulting to the directory's own name. Relative paths anywhere in the
file resolve against the file's directory, so a config can sit beside the
builds it names.

```bash
scripts/benchmark/meta/meta-cluster-bench.py --config my-cluster.toml
scripts/benchmark/meta/meta-cluster-bench.py --config my-cluster.toml --port-base 29101
```

## What both Python scripts guarantee

They start their cluster under the same fixed `[raft_config]`, which a config
file's own `[raft_config]` overrides knob by knob rather than replacing. Two
runs that leave it alone are comparable with each other.

Their work dir (`./.databend` by default) collects node configs, raft dirs,
node logs, and the workload's own output as timestamped `metabench-*.log` and
`lua-*.log` files, one per run, so no run overwrites an earlier run's raw
output. The work dir survives between runs, so successive runs pile data onto
the same cluster.

## databend-metabench

The load generator behind `meta-cluster-bench.py`
(source in `src/meta/binaries/metabench/`). Run it directly against a cluster
you started yourself:

```bash
./target/release/databend-metabench --grpc-api-address 127.0.0.1:9191 \
  --client 10 --number 1000 --rpc upsert_kv

./target/release/databend-metabench --grpc-api-address 127.0.0.1:9191 \
  --client 10 --number 100000 --run-secs 60 \
  --rpc 'batch_create_tables:{"batch":1000,"db_size":1000}'
```

`--rpc` selects the workload, and several of them take a JSON config after a
colon, as the second example shows. `--run-secs` caps the run by wall clock
time, while `--number` still bounds how many operations each client issues. Run
`databend-metabench --help` for the full list of workloads.

## reports/

Past benchmark write-ups, kept so their numbers stay reproducible against the
script version that produced them.
