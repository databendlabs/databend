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

Each original block is an independent sorted stream. A task partitions its inputs into
row-balanced groups. Each group is a separate source processor: it incrementally reads,
decodes and merges its original inputs using the existing key encoding and loser tree.
The second level consumes the resulting sorted streams through pipeline input ports. It
performs no storage reads; missing heads return `NeedData`, not a blocking receive.
Only the final globally sorted stream enters ordered compact and parallel serialization.
Small or memory-constrained tasks use one group and omit the second level. The executor
neither sorts each input again nor concatenates overlapping blocks into one supposedly
sorted route.

The node still receives at most one task per round of a recluster statement. Parallelism
is restored *inside* that task. Group count is capped by input count, `max_threads` and
memory-admitted fan-in. The task budget is computed once: all groups together get half;
second-stage retention gets a quarter; the remaining quarter covers inter-stage delivery
and output construction. Group budgets are not additional per-query memory allowances.
These are estimates; decoder pages and backend buffers can exceed them as described below.

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
chain remains in use. Optional inverted-index reuse is described below. Internal lineage
columns identify original source ordinals and absolute row offsets through all spill rounds.
They are extracted only after the final compact concat/split. Lineage is enabled only for
an admitted opt-in index merge; row-count-changing aggregate-state reaggregation cannot
use it for index reuse.

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
- The final merge remains one core, but groups read/decode concurrently. Intermediate group
  materialization adds a gather step; high-interleaving wide rows can still regress. Group
  count and batching can affect CPU parallelism, buffering and spill behavior.
- Streaming range IO in the standalone change does not use the later shared disk-cache wrapper.
  OpenDAL/backend buffers are additional to our window buffers; 1 MiB alignment is not a hard
  total-memory bound. Storage operation duration for a stream includes its response lifetime
  and consumer pauses, so it cannot be interpreted as consumer blocking time.

Do not enable this globally on the assumption that every recluster becomes faster. Compare
identical input snapshots, storage/cache conditions and memory settings for the target workload.
Keep the switch off if time regression outweighs the observed memory benefit.

## Optional inverted-index reuse

The follow-up integration is independently disabled by default:

```sql
SET enable_recluster_multiway_merge = 1;
SET enable_recluster_inverted_index_merge = 1;
ALTER TABLE my_table RECLUSTER FINAL;
```

It only applies to horizontal linear `MergeBlocks` using the two-level executor. SortBlocks,
vertical tasks, unsupported schemas and row-count-changing aggregate-state reaggregation
retain the old index-building behavior. Source index metadata is transported in task part
order only when the setting is enabled; old serialized tasks default to no source indexes.
Compatibility is per index: every source must have the current sync index definition version,
current outer file format and a nonempty bundle. Missing/old definitions rebuild normally,
so one output can contain a merged existing index and a rebuilt newly declared index.

When at least one index is admitted, original-source lineage follows complete rows through
both merge levels and spill. After final compact establishes block boundaries, the internal
columns are removed and ranges are attached to that block only. Serialization skips admitted
index builders but builds all others. Written block metadata and its exact row mapping stay
in a local envelope. A task-level collector holds envelopes until every selected index merge
finishes, then emits ordinary AppendBlock mutation logs. It retains metadata/mappings, not
the complete output payload. Parallel serialization completion order is allowed: each bundle
uses its associated block mapping, not an assumed globally ordered completion ordinal.

The existing Tantivy merger reads each source once per index and merges postings, positions,
fieldnorms and JSON fast fields without re-tokenizing. The current expected schema is checked
before any destination index is created, and source doc counts are verified against row counts.
A selected bundle's IO/corruption/schema error fails the task; it does not silently rebuild.
No mutation metadata is released before all merges succeed. Already-written uncommitted data
or index objects on failure remain subject to existing orphan cleanup; the snapshot stays
unchanged. Cancellation is checked while collecting, between indexes and before publication,
not inside every Tantivy postings operation or blocking receive.

Resource admission is deliberately conservative and not a hard memory cap: index working
allowance is 10% of available node/query memory, at most 128 MiB. Estimate includes 64 B/source
row, twice the largest per-index sum of bundle bytes, and 16 MiB per expected output. Actual
range-vector capacity and extra output count are checked while collecting. Inputs beyond
UInt32 doc limits or outputs beyond UInt16 ordinal limits are not admitted. The current merger
still expands doc mappings and holds all outputs; sibling sizes, decompression, postings scratch
and backend buffers can exceed the estimate. Default-off rollout requires workload measurements,
object-store/multi-node tests, write-fault injection and human review. Do not advertise this
setting as a guaranteed speedup or bounded-memory external index merge.

Regression coverage: search results before/after, doc-to-row mapping with duplicate keys and
computed cluster keys, forced spill, direct two-output collector metadata, mixed reuse/rebuild,
missing/old source definitions, low-budget fallback, schema rejection before output creation,
cancel-before-merge and corrupt-source failure with unchanged snapshot. SQL suite:
`09_0055_recluster_inverted_merge.test` (service runner execution still required).

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
| Index merge allowance | 10% of available memory, cap 128 MiB | Independent estimate; not a hard combined peak guard. |
| Index merge row estimate | 64 B per source row | Mapping/origin estimate; variable range vectors counted separately. |
| Index merge bundle estimate | 2 × largest per-index bundle total | Sibling payload and decompression scratch not strictly covered. |
| Index merge output reserve | 16 MiB per output | Tantivy writer estimate, requires workload validation. |
| Merge head/retention share | budget / 2 | Soft retention threshold, not an allocation guard. |
| Output batch byte estimate | budget / 4 | Uses average uncompressed row width, not maximum row width. |
| Batch row cap | 8192, also capped by max_block_size | Tradeoff between buffering and work/IO handoffs. |
| Fan-in cap / minimum | 64 / 2 | Open-reader and memory scaling; one route uses an empty companion. |
| Batch / fan-in reduction | halve, minimum 1 row / 2 routes | Discrete policy can overcorrect. |
| Minimum-mode headroom | max source rows × average row bytes <= row budget / 2 | Conservative estimate, not page-size knowledge. |
| Maximum first-stage groups | min(max_threads, input count, max(1, admitted fan-in / 4)) | Conservative admission; not a new thread pool. |
| Group budgets | task budget / 2 / groups | Keeps total group allowance at half the task target. |
| Second-stage retention | task budget / 4 | Oversized head set fails explicitly instead of stalling. |
| Group input rows | task batch / (2 × groups), at least 1 | Conservative per-group buffering; natural minimum batches disabled for multi-group execution. |
| Group fan-in | ceil(task fan-in / groups), at least 2 | Do not halve twice: it unnecessarily spills small groups. |
| Group output rows | min(task output rows, group budget / 4 / estimated row bytes) | Independent of small input batches to avoid tiny spill files. |
| Removed fixed horizontal window | 256 KiB | Previously coupled delivery windows to backend range requests. |
| Removed DEFAULT_WINDOW_SIZE | 4 MiB | No production byte-window fallback remains. |

Merge range gap/size limits use `storage_io_min_bytes_for_seek` and
`storage_io_max_page_bytes_for_read` (existing defaults 48 B / 512 KiB), not new constants.
`RangeMerger` can exceed its size limit by one input window. Backend streaming buffers
(e.g. OpenDAL filesystem's current 2 MiB read buffer) are library policy, not controlled by
these limits. Window rounding and fan-in admission need further workload validation.

## Two-level verification

Original-source lineage remains `(source_ordinal, absolute_row_offset)` through both levels
and every spill round. An encoded key, when necessary, is appended *after* lineage and
removed by the second level. The second-stage key schema includes lineage fields so its
order-column offset cannot accidentally select the source ordinal. Only after final
compact concat/split are lineage ranges extracted. Equal keys need not be stable, but
payload and source identity always move together. `build_pipeline(..., lineage=true)` is
used only when the optional index merge admits at least one definition.

Tests run the real three-group/input-port pipeline with forced spill, variable and simple
key encodings, equal keys and compact replay. They assert original ordinals appear exactly
once in the group assignments, budgets sum to at most half the task target, and final rows
replay to the exact original payload. Executor thread count is explicitly set to one for
these tests to detect blocking inter-group waits. A separate test rejects an oversized
second-stage head set without waiting forever.

Optimized-test measurements, 16 inputs and 2,097,152 rows, local filesystem, lineage disabled:

| Payload | Original | Two-level | Original query peak | Two-level query peak |
| --- | --- | --- | --- | --- |
| ~2.58 GiB repeated strings, forward | 1.296 s | 1.391 s | 3,885,536,567 B | 1,842,894,752 B |
| ~2.58 GiB repeated strings, reverse | 1.224 s | 1.401 s | 3,814,010,193 B | 1,855,248,306 B |
| 2 GiB salted hash strings, forward | 0.845 s | 0.787 s | 5,114,855,782 B | 1,214,487,820 B |

The single-stage streaming implementation took ~2.87 s on repeated strings. Two levels
reduce that regression substantially, but repeated-string elapsed remains ~7-14% above the
old path in these limited runs. Hash-string tracked reads remain 48 for both paths. Peak
memory increases versus the single-stage implementation but remains below the old path.
Do not turn these local observations into a universal speedup or production release claim.
An initial group fan-in division by `2 × groups` caused unnecessary intermediate runs and
~12 s elapsed; corrected to `ceil(task fan-in / groups)` before these measurements.

## Historical single-stage streaming IO verification

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
