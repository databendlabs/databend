# Databend Meta Dependency Optimization Benchmark

Date: 2026-05-15

This rerun supersedes the 2026-05-14 result and adds a 1024-client setting.

## Scope

This benchmark compares the old Databend `ups/main` baseline before the Databend Meta dependency update with the current optimized Databend branch after the released Databend Meta tags were merged.

The benchmark target is Databend Meta in a 2-node cluster. The measured workload is `upsert_kv`.

## Binary And Dependency Versions

Current optimized binary:

| item | version |
|---|---|
| Databend binary version | `1.2.910-nightly` |
| Databend source commit used to build the benchmark binary | `221ecd54b7` |
| min-compatible-client-version | `1.2.676` |
| data-version | `V004(2024-11-11: WAL based raft-log)` |
| databend-meta | `v260512.1.0` |
| databend-meta-client | `v260205.9.0` |
| openraft | `0.10.0-alpha.20` |
| raft-log | `0.4.2` |

Baseline binary:

| item | version |
|---|---|
| Databend binary version | `1.2.910-nightly` |
| Databend source commit used to build the benchmark binary | `a2661824e3` |
| min-compatible-client-version | `1.2.676` |
| data-version | `V004(2024-11-11: WAL based raft-log)` |
| databend-meta | `v260428.3.0` |
| databend-meta-client | `v260205.8.0` |
| openraft | `0.10.0-alpha.19` |
| raft-log | `0.4.0` |

The benchmark client is the current optimized `databend-metabench` binary for both sides. The current rows were rerun after `databend-metabench` switched its benchmark-local latency histogram to `base2histogram`; the baseline rows keep the earlier raw reports. The leader and follower in each run use the same server build.

## Dependency Optimization

The optimized branch uses released Databend Meta tags that include the `raft-log` WAL flush optimizations:

- bounded group-commit wait for WAL flushes;
- configurable flush batch size;
- vectored batch writes;
- WAL flush latency percentiles exposed through raft-log stats;
- richer meta service logging for raft-log and Openraft runtime stats.

## Benchmark Setup

All runs use `scripts/benchmark/meta-cluster-bench.py` with a 2-node Databend Meta cluster. Workload is `upsert_kv`.

| clients | build | ops/client | total ops | client pool |
|---:|---|---:|---:|---:|
| 4 | both | 10000 | 40000 | 4 effective |
| 64 | both | 625 | 40000 | 16 |
| 256 | both | 156 | 39936 | 16 |
| 1024 | baseline | 39 | 39936 | 16 |
| 1024 | current | 156 | 159744 | 16 |

Raw reports and logs were generated under:

```text
/Users/drdrxp/xp/vcs/sessions/stream/bench-reports/databend-meta-final-20260515
```

## Result

| clients | build | qps | delta | wall ms | avg us | p50 us | p90 us | p99 us | max us | slow RPC | slow IO leader/follower |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | baseline | 172.9 | - | 231366 | 23129 | 22000 | 30000 | 60000 | 955214 | 81 | 210/89 |
| 4 | current | 193.8 | +12.1% | 206447 | 20637 | 19362 | 23634 | 33792 | 817213 | 61 | 101/92 |
| 64 | baseline | 2807.0 | - | 14250 | 22656 | 22000 | 30000 | 45000 | 992925 | 93 | 96/6 |
| 64 | current | 3210.8 | +14.4% | 12458 | 19806 | 19643 | 24278 | 30212 | 47985 | 0 | 0/0 |
| 256 | baseline | 8920.3 | - | 4477 | 28321 | 20000 | 25000 | 200000 | 1183675 | 257 | 162/4 |
| 256 | current | 12127.5 | +36.0% | 3293 | 20649 | 19567 | 24897 | 39823 | 90760 | 0 | 0/0 |
| 1024 | baseline | 24262.5 | - | 1646 | 39864 | 35000 | 45000 | 300000 | 367682 | 0 | 0/0 |
| 1024 | current | 23440.1 | -3.4% | 6815 | 42854 | 41693 | 57744 | 80630 | 298543 | 0 | 0/0 |

## Raft-log Stats

The new raft-log stats are available only in the current optimized build.
For the 1024-client current rerun, the server-side raft-log and runtime rows
use the last stats sample emitted by the server logs; the client-side summary
in the Result table is the final metabench summary.

| clients | avg batch | max batch | avg group wait us | avg queued wait us/write | avg write us/batch | avg sync us/batch | sync p99 us | batch p99 us | submitted->persisted p90/p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 1 | 4 | 1297 | 4868 | 3772 | 5556 | 11757 | 15856 | 18845/23096 |
| 64 | 19 | 55 | 1222 | 4974 | 3315 | 5605 | 11878 | 15250 | 20211/24714 |
| 256 | 54 | 144 | 1150 | 4782 | 3410 | 5425 | 11672 | 13986 | 20067/23043 |
| 1024 | 99 | 268 | 1160 | 4386 | 3089 | 5195 | 11221 | 13759 | 16452/19245 |

## Stage Latency

| clients | build | submitted->persisted p90/p99 us | proposed->applied p90/p99 us |
|---:|---|---:|---:|
| 4 | baseline | 15780/21060 | 24422/29465 |
| 4 | current | 18845/23096 | 20515/26762 |
| 64 | baseline | 22213/46811 | 25956/46811 |
| 64 | current | 20211/24714 | 23295/26605 |
| 256 | baseline | 22035/25485 | 25863/28206 |
| 256 | current | 20067/23043 | 22676/24424 |
| 1024 | baseline | 33916/46946 | 48169/54753 |
| 1024 | current | 16452/19245 | 56281/57243 |

## Interpretation

The current optimized dependency stack improves throughput in the 4, 64, and 256 client runs:

- 4 clients: `172.9 -> 193.8 qps`, `+12.1%`;
- 64 clients: `2807.0 -> 3210.8 qps`, `+14.4%`;
- 256 clients: `8920.3 -> 12127.5 qps`, `+36.0%`.

The 64-client and 256-client runs show the clearest win: the current build removes the slow RPC and slow IO outliers seen in the baseline run, and p99 client latency improves from `45ms -> 30.2ms` at 64 clients and from `200ms -> 39.8ms` at 256 clients.

The 1024-client current run was rerun as `1024 clients x 156 ops/client`, about `6.8s` total, after switching `databend-metabench` from the old manual latency buckets to `base2histogram`. Client-side qps is slightly lower than the old baseline row (`24262.5 -> 23440.1 qps`, `-3.4%`) and p90 client latency is worse (`45ms -> 57.7ms`). The more accurate client-side p99 is `80.6ms`; the previous `300ms` value was the old coarse bucket upper bound, not a precise percentile estimate.

On the server side, the rerun still shows a faster persistence stage: `submitted->persisted` p90 drops from `33916us` to `16452us`. The remaining 1024-client latency is mainly after persistence: `proposed->applied` p90 is `56281us`, with a large `persisted->committed` component in the runtime stats.

The persistence path is still a major cost. In the optimized build, `submitted->persisted` remains roughly `16ms to 20ms` at p90 across these runs. The current optimization mainly amortizes the sync cost by batching more writes per WAL flush; it does not eliminate the underlying fsync latency.

## Raw Files

| run | report |
|---|---|
| baseline 4 clients | `baseline-c4-pool16.report.txt` |
| current 4 clients | `current-c4-pool16-base2histogram.report.txt` |
| baseline 64 clients | `baseline-c64-pool16.report.txt` |
| current 64 clients | `current-c64-pool16-base2histogram-r2.report.txt` |
| baseline 256 clients | `baseline-c256-pool16.report.txt` |
| current 256 clients | `current-c256-pool16-base2histogram.report.txt` |
| baseline 1024 clients | `baseline-c1024-pool16.report.txt` |
| current 1024 clients | `current-c1024-n156-pool16-base2histogram.report.txt` |
