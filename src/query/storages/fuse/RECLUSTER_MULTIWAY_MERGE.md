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

The read chain is `ChunkedRangeReader -> MergeRangeReader -> OperatorRangeReader` in
streaming mode. The merge layer coalesces adjacent consumer windows within each column
using the existing storage IO settings; it does not merge separate columns' response streams.
The storage tail opens a continuous column-range stream and delivers bounded segments from
that stream, instead of issuing a backend range request per consumer window. Cache is not
required and is not inserted by the new executor. `Box<dyn RangeReader>` supports optional
layer composition. A discontinuity or cancelled partial read invalidates the stream cursor.

Each physical column's delivery window is computed as
`ceil(compressed_chunk_bytes * min_rows / block_rows)`, rounded up to a 1 MiB multiple and
clipped to the chunk length. Intermediate arithmetic uses `u128`. This aligns sizes, not file
offsets. The production reader requires target window rows; there is no fixed byte fallback.
A byte-window override exists only for tests. Input batches adapt to estimated row width,
metadata-derived compressed windows and the number of input routes. Output batch rows
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
- Streaming range IO in the standalone change does not use the later shared disk-cache wrapper.
  OpenDAL/backend buffers are additional to our window buffers; 1 MiB alignment is not a hard
  total-memory bound. Storage operation duration for a stream includes its response lifetime
  and consumer pauses, so it cannot be interpreted as consumer blocking time.

Do not enable this globally on the assumption that every recluster becomes faster. Compare
identical input snapshots, storage/cache conditions and memory settings for the target workload.
Keep the switch off if time regression outweighs the observed memory benefit.

## Parameters requiring workload review

These choices are not demonstrated production-optimal values. Track them separately from
correctness limits and existing user settings:

| Choice | Current value | Purpose / risk |
| --- | --- | --- |
| Row-derived window alignment | 1 MiB | May overallocate for small target batches; 16 MiB not adopted. |
| Horizontal prefetch lookahead | 1 | Limited overlap; 2 tested separately, not made default. |
| Low-level reader default prefetch | 2 | Used outside the horizontal override. |
| Merge tail/slot capacity | prefetch + 1 | Counts segments, not bytes. |
| Retained-segment budget multiplier | 3 | Estimate for hinted/recent segments; backend buffers are additional. |
| Task working-memory fraction | 30% of currently available memory | Remaining 70% is only a reserve estimate for pages/downstream. |
| Task working target cap | 256 MiB | Can cause external rounds even on larger nodes. |
| Merge head/retention share | budget / 2 | Soft retention threshold, not an allocation guard. |
| Output batch byte estimate | budget / 4 | Uses average uncompressed row width, not maximum row width. |
| Batch row cap | 8192, also capped by max_block_size | Tradeoff between buffering and work/IO handoffs. |
| Fan-in cap / minimum | 64 / 2 | Open-reader and memory scaling; one route uses an empty companion. |
| Batch / fan-in reduction | halve, minimum 1 row / 2 routes | Discrete policy can overcorrect. |
| Minimum-mode headroom | max source rows × average row bytes <= row budget / 2 | Conservative estimate, not page-size knowledge. |
| Removed fixed horizontal window | 256 KiB | Previously coupled delivery windows to backend range requests. |
| Removed DEFAULT_WINDOW_SIZE | 4 MiB | No production byte-window fallback remains. |

Merge range gap/size limits use `storage_io_min_bytes_for_seek` and
`storage_io_max_page_bytes_for_read` (existing defaults 48 B / 512 KiB), not new constants.
`RangeMerger` can exceed its size limit by one input window. Backend streaming buffers
(e.g. OpenDAL filesystem's current 2 MiB read buffer) are library policy, not controlled by
these limits. Window rounding and fan-in admission need further workload validation.

## Streaming IO verification

The earlier window-per-request measurements below are historical, not current executor
results. After adding the merge layer, continuous storage streams, metadata-derived windows
and 1 MiB size alignment, a local optimized-test probe with 16 interleaved inputs and
2,097,152 rows produced:

| Payload | Original elapsed | Multiway elapsed | Original tracked reads | Multiway tracked reads | Original peak | Multiway peak |
| --- | --- | --- | --- | --- | --- | --- |
| 2 GiB salted hash strings, forward | 0.847 s | 0.893 s | 48 | 48 | 5,188,404,800 B | 944,971,313 B |
| 2 GiB salted hash strings, reverse | 0.842 s | 0.868 s | 48 | 48 | 5,045,970,338 B | 978,805,223 B |
| ~2.58 GiB repeated strings, forward | 1.256 s | 2.867 s | 65 | 96 | 3,755,535,164 B | 1,225,853,035 B |
| ~2.58 GiB repeated strings, reverse | 1.194 s | 2.890 s | 65 | 96 | 4,155,334,769 B | 1,229,098,437 B |

Reads include metadata and subsequent recluster rounds. Source compressed bytes are equal;
metadata differences account for small total-byte differences. Before streaming, the hash
fixture used 8,304 total reads (8,288 source ranges); it now uses 48. Unit tests independently
verify one backend range for multiple consumption windows, including saturated hint batches.
The repeated-string fixture remains decoder-bound with one source core; its time regression
is not resolved by the IO fix. Prefetch remains 1; a separate fixed-window probe with depth 2
was insufficient to justify changing it. These limited filesystem observations are not
production release or object-store performance guarantees.

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

Historical pre-streaming optimized-test results (not the production release profile), 16 interleaved sources,
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
