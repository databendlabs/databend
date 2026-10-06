# Horizontal recluster multiway merge

`enable_recluster_multiway_merge` is an experimental, default-off execution switch:

```sql
SET enable_recluster_multiway_merge = 1;
ALTER TABLE my_table RECLUSTER FINAL;
SET enable_recluster_multiway_merge = 0;
```

Only ordered, linear `MergeBlocks` tasks using Parquet and without virtual computed fields
use the new executor. `SortBlocks`, other clustering layouts, and unsupported schemas retain
the original path. On branches with a vertical recluster executor, an explicitly selected
vertical task keeps that executor. The switch does not change persisted formats or task
serialization. Session settings travel to workers in the existing Flight query environment.

## Execution

Each original block is an independent sorted stream. Complete rows are decoded incrementally
using the existing range reader and compared using the ordinary sort-key encoding and loser
tree. The executor neither sorts each input again nor concatenates overlapping blocks into
one supposedly sorted route.

Input batches adapt to estimated row width and the number of input routes. Output batch rows
are separate. Fan-in is reduced only after reducing input batch rows; when not all routes fit,
ordered intermediate runs are spilled as sequences of chunks and merged in further rounds.
`force_sort_data_spill` and the existing sort-spill enable flag are respected. With spill
disabled, insufficient working memory is an error.

A merge interruption preserves each unconsumed decoded head, releases original readers, and
spills the remaining streams with lower fan-in. Reopening a reader skips the already-decoded
prefix without counting it as a second logical scan. IO, decoding, and ordering errors fail
the task; they are not interpreted as EOF or silently retried by rebuilding indexes.

The ordinary ordered compact, statistics, index rebuilding, serialization and snapshot commit
chain remains in use. This change does not reuse inverted indexes. Optional internal lineage
columns identify original source ordinals and absolute row offsets through all spill rounds.
They are extracted only after the final compact concat/split. Production lineage is disabled;
row-count-changing aggregate-state reaggregation cannot use it for index reuse.

## Minimum batch semantics

`read_rows(n)` returns exactly `n` rows or fails on premature EOF. `read_min_rows(n)` consumes
whole logical decoding batches until at least `n` rows have been obtained, returning a shorter
tail at EOF. It does not split the last logical batch.

For complete rows, all physical logical columns perform their initial minimum read. The
maximum resulting row count becomes a fixed alignment target. Shorter columns read exactly
the missing rows, retaining any new decoding remainder. Synthetic defaults and origin fields
are generated for that target. Alignment does not keep increasing the target: unrelated page
boundaries would otherwise chase each other toward the end of the block.

Nested physical leaves can have different page boundaries. The guarantee concerns assembled
logical-column batches, not draining every physical leaf's decoder. The complete-row executor
uses exact batches when its estimated per-route budget cannot safely accommodate minimum
batches. Neither interface is nonblocking: a prefetch miss waits for IO synchronously.

## Resource and rollout limits

The executor targets a task-local fraction of available node/query memory, capped at 256 MiB,
and reserves room for output construction and existing downstream consumers. This is a working
budget, **not a hard end-to-end byte limit**. In particular:

- Parquet pages and dictionaries are decoded before post-decode size checks. A single oversized
  page can exceed the target; reducing requested rows cannot prevent that allocation.
- Decoded backing buffers, output gather, compact, index rebuilding and serialization consume
  additional memory. Existing backing-buffer GC is applied at the new path's batch boundaries.
- The downstream block still needs complete serialization/upload before publishing metadata.
- IO waits are synchronous. Cancellation is checked at work boundaries, not during every
  blocking receive.
- Each task has one merge core. Reading/decoding and high-interleaving payload selection may
  lose CPU parallelism compared with the original sorting pipeline.
- Direct range IO in the standalone change does not use the later shared disk-cache wrapper.

Do not enable this globally on the assumption that every recluster becomes faster. Compare
identical input snapshots, storage/cache conditions and memory settings for the target workload.
Keep the switch off if time regression outweighs the observed memory benefit.

## Validation and performance probe

```bash
cargo test -p databend-common-settings --lib test_recluster_multiway_merge_setting
cargo test -p databend-common-pipeline-transforms --test it merger
cargo test -p databend-common-storages-fuse --lib low_level_block_reader
cargo test -p databend-query --lib horizontal_recluster -- --test-threads=1
cargo clippy -p databend-query -p databend-common-storages-fuse \
  -p databend-common-pipeline-transforms -p databend-common-settings --all-targets -- -D warnings
```

SQL coverage is in `09_0055_horizontal_merge.test`. Run it against both a standalone service
and a cluster, together with `09_0055_recluster_task_kinds.test` and
`09_0011_change_tracking.test`. Tests cover spill rounds, suffix recovery, equal keys, lineage
replay, defaults, computed cluster keys, nullable float/Decimal composite keys, nested payload,
stream origins, ordering errors, missing input, cancellation checks and unchanged snapshots
on an execution-time corrupt-input failure.

The ignored local probe compares separately generated, identically ordered input tables:

```bash
RECLUSTER_BENCH_ROWS=32768 RECLUSTER_BENCH_PAYLOAD_REPEAT=256 \
CARGO_PROFILE_TEST_OPT_LEVEL=3 CARGO_PROFILE_TEST_DEBUG=0 \
CARGO_PROFILE_TEST_INCREMENTAL=false \
cargo test -p databend-query --lib benchmark_horizontal_merge_wide_rows \
  -- --ignored --nocapture --test-threads=1
```

Local optimized-test results (not the production release profile), 16 interleaved sources,
524,288 rows, filesystem storage, lineage disabled:

| Payload | Original elapsed | Multiway elapsed | Original query peak | Multiway query peak |
| --- | --- | --- | --- | --- |
| ~597 MiB string data | 0.413 s | 0.790 s | 1,433,325,734 B | 1,019,583,538 B |
| ~2.3 MiB string data | 0.189 s | 0.227 s | 81,734,408 B | 79,307,667 B |

These are limited single-run observations, not stable performance claims. Query peak is tracked
allocation, not process RSS. The old path ran first; cache-order effects and build profile must
be controlled before rollout. The observed wide-payload time regression remains unresolved.
Release-profile measurements, object-store tests, blocked-IO cancellation and write-failure
injection remain rollout requirements. Default-off does not remove the need for human review
and CI before merge.
