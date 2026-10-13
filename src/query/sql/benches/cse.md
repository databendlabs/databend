# Nested scalar CSE benchmark

`cse.rs` compares three execution plans:

- `no_cse`: evaluate each output independently.
- `optimized`: use the real `apply_cse` implementation, including candidate pruning.
- `manual_pruned`: hand-build a plan that materializes only each small repeated
  parent, leaving its large children inline. This is an oracle for production CSE.

Inputs have 1024 rows. Each independent chain appears twice in the output list:

- String: `octet_length(repeat(a, repeat_count))`, with 64-byte row-varying input.
- JSON: `is_object(parse_json(concat(a, whitespace)))`, with row-varying JSON input.

Different constants distinguish the 8 independent chains. Test sizes are 256,
4096 and 65536 bytes (approximate per-row intermediate/payload size).
Parsing, constant folding, plan construction and equality checks are outside
measurement. Execution includes input handle cloning and output destruction.
All three plans are checked for equal outputs in every case.

## Run

```bash
cargo bench -p databend-common-sql --bench cse
CSE_MEMORY_ONLY=1 cargo bench -p databend-common-sql --bench cse
```

The local optimized run used `CARGO_PROFILE_BENCH_LTO=false`,
`CARGO_PROFILE_BENCH_DEBUG=0` and `CARGO_PROFILE_BENCH_CODEGEN_UNITS=16` to reduce
build time; optimization otherwise follows the repository profile (`opt-level=s`).
The local `protoc` installation needed `PROTOC_INCLUDE` pointing to the standard
protobuf definitions already installed in the Cargo registry.

To repeat large cases with more samples, run the executable printed by Cargo:

```bash
/path/to/cse-executable --bench 65536 --sample-count 10 --sample-size 1 --max-time 15
CSE_MEMORY_ONLY=1 /path/to/cse-executable
```

## Local exploratory results before implementing pruning

The following baseline compares the former production CSE (`Current`) with the
hand-built pruning plan (`Pruned`). Optimized build, 65536-byte cases, median of
10 single-iteration samples:

| Kind | Chains | No CSE | Current | Pruned | Current peak | Pruned peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| String | 1 | 53.74 ms | 24.98 ms | 24.68 ms | 80.02 MiB | 80.02 MiB |
| String | 8 | 395.9 ms | 220.1 ms | 194.8 ms | 640.13 MiB | 80.24 MiB |
| JSON | 1 | 119.9 ms | 59.74 ms | 57.44 ms | 200.15 MiB | 200.15 MiB |
| JSON | 8 | 959.4 ms | 591.7 ms | 471.1 ms | 1601.21 MiB | 200.15 MiB |

Peaks are execution-only allocations measured separately using Databend's
`TrackingGlobalAllocator` and `MemStat`. Input/plan allocations are excluded.
The tracker buffers changes at 4 MiB granularity: peaks are approximate and small
cases can even report zero. These are not process RSS or summed column sizes.

Single-chain pruning shows no meaningful peak reduction. Eight chains reduce
peak by about 87.5%, and elapsed time by roughly 11.5% (String) / 20.4% (JSON).
The same expensive operations still execute once per chain in both CSE plans;
the difference is intermediate lifetime and plan boundaries, not reduced
function evaluation count. These constructed cases establish a possible
memory-lifetime benefit, not the frequency or benefit in real workloads.

## Production pruning verification

After adding effective-reference pruning to `apply_cse`, the same optimized
build and 10-sample large-case run produced:

| Kind | Chains | Production pruning | Manual oracle | Production peak | Oracle peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| String | 1 | 25.39 ms | 24.59 ms | 80.02 MiB | 80.02 MiB |
| String | 8 | 202.1 ms | 198.9 ms | 80.24 MiB | 80.24 MiB |
| JSON | 1 | 59.05 ms | 58.68 ms | 200.15 MiB | 200.15 MiB |
| JSON | 8 | 473.9 ms | 473.8 ms | 200.15 MiB | 200.15 MiB |

All 12 input configurations passed output equality and candidate-count checks.
Memory agrees at the displayed precision (the largest difference between the
large-case production and oracle measurements was 4 bytes). The timing results
are exploratory, not a guarantee of these exact improvements on other machines.

## Paired timing recheck

Run `CSE_PAIRED_ONLY=1 /path/to/cse-executable` for an AB/BA comparison. This
reuses both plans and the same input in one process, warms up each plan four
times, then measures 30 pairs per case, alternating which plan runs first.
Plan construction and output validation remain outside timing. Plan assertions
check identical parent expressions, projections and output references modulo
candidate permutation and temporary-column display names. The production plan
uses hash-dependent order for equal-size candidates; the oracle uses input order.

Three fresh process runs (90 samples per plan/case) yielded these pooled medians:

| Kind | Chains | Production pruning | Manual oracle | Difference |
| --- | ---: | ---: | ---: | ---: |
| String | 1 | 25.56 ms | 25.66 ms | -0.36% |
| String | 8 | 203.59 ms | 204.71 ms | -0.55% |
| JSON | 1 | 58.21 ms | 58.43 ms | -0.37% |
| JSON | 8 | 463.97 ms | 464.03 ms | -0.01% |

For 8-chain cases, each individual run's difference between medians ranged from
-0.47% to +1.04% for String, and -0.56% to +0.22% for JSON. The previous apparent
String gap did not reproduce as a consistent production slowdown. These local
measurements do not establish a statistically significant difference.
