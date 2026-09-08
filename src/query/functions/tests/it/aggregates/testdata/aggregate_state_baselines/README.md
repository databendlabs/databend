# Aggregate state baselines

Each family declares its cases directly in `test_state_baselines()` in its
existing aggregate test file under `tests/it/aggregates/`. Families with separate
test files, such as moving
sum/avg and the geometry aggregates, keep their declarations in those files.
`support/aggregate_state_baseline_support.rs` defines the shared declarations and type
loading; `support/aggregate_state_baselines.rs` provides shared compatibility checks. Each
existing test file owns its test entry points; there is no central family list.
Historical payloads are inline in those declarations; this directory holds only
this documentation.

The source is aggregate-v1 at revision
`cf0dd517a8bb4151c409d956b04386ceb001b4c5`. The retained declarations select
representative implementation branches rather
than enumerate supported signatures: 256 calls, with all 182 previously captured
state samples retained. The 18 return types missing from the original inventory
were filled from the same revision's v1 rules: min/max/any and median preserve
Decimal precision/scale with an outer nullable result; multi-argument uniq
returns UInt64.
Each family file explains its selection against the corresponding implementation.
Numeric widths sharing a kernel and duplicate aliases are not expanded; distinct
accumulator types, decimal storage widths, state encodings, result policies and
nullable adaptors remain represented. Selection is explicit in each family,
without a global representative-type list or automatic registry expansion.
These counts describe the current selection, not a coverage requirement.

## Family declarations

Each family uses two `Case` variants in the same list:

- `Case::Metadata { expression, arguments, result, state }` checks the saved
  return and state types, without declaring samples.
- `Case::Samples { expression, arguments, result, state, samples }` performs
  the same metadata checks and additionally checks historical states and
  behavior. Its sample list must be nonempty.

There are 185 metadata-only cases and 71 cases containing 182 samples. Each
of the 256 calls is declared once, with a required return type. Both variants
state the saved call and types explicitly and use the shared call parser,
without looking up another case's declaration or the current implementation.

All cases check the ordinary, `_state`, `_merge`, and `_merge_state` routes.
Samples additionally check reading and merging historical payloads. A
metadata-only call has no payload compatibility evidence.

The expression uses `x0`, `x1`, etc., whose types are explicitly declared in
argument order. Empty and all-NULL samples never infer their input types.
The runner prepares the declarations by parsing type names.

All retained samples have the same captured result for ordinary aggregation and
reading historical state. The migration verified both against the original data.

Samples use named `Sample { label, inputs, state, result, merge_result }` fields.
`merge_result` explicitly selects `MergeResult::Skip`, `MergeResult::SameAsResult`,
or `MergeResult::Value(...)`. The latter two check a saved state merged with a
freshly built one against the captured combined result. `SameAsResult` reuses
the saved ordinary result where the captured combined result is identical;
it does not infer the combined result by doubling the ordinary result.

Type declarations use SQL names such as `Nullable(Int64)` and
`Tuple(Array(Decimal(38, 0)), Boolean)`. The existing
`resolve_type_name_by_str(name, true)` parses them into `TableDataType`, which is
converted to `DataType`. The `true` preserves non-nullable defaults, including
nested types; nullability is explicit in each declaration. Scalar values still
use the expression crate's existing Rust types. The `AggregateState` wrapper is constructed from the saved call and saved state
layout, never obtained from the implementation being tested. Declarations and
payload loading do not construct an aggregate.

Calls use the existing SQL AST parser. This stage accepts plain calls with named
column arguments only. DISTINCT, FILTER, ORDER BY, windows, explicit parameters,
and computed arguments are rejected before any conversion can discard them.
AVG remains outside the inventory. Intrinsic `uniq` and `approx_count_distinct`
are not SQL DISTINCT modifiers and remain included.

## Historical payloads

Inputs and Array states are constructed directly with the existing column
`from_data`, `from_opt_data`, and decimal `from_data_with_size` /
`from_opt_data_with_size` methods. Empty arrays retain their element type, and
nullable inputs retain their validity. Arrow IPC framing is not part of these
fixtures anymore: the migration verified each decoded type and element value.

Actual Binary/Variant/Geometry bytes use inline base64 literals. This includes
binary or geometry elements inside typed arrays: only the outer Arrow wrapper
was removed. Helpers decode with the existing base64 crate; scalar helpers such
as `int64`, `decimal64` and `tuple` only construct existing expression values.
There is no external payload directory or fixture codec. These samples are not
full Fuse block fixtures.

Keep historical bytes unchanged. Do not regenerate expected states with the
current implementation to make a compatibility test pass. New captures should
record their source explicitly. Decimal values preserve their raw integers and
captured precision/scale; do not rescale them during loading. Tests compare
results and captured metadata, not arbitrary raw state equality.

## Validation

Each of the 30 test files has its own `test_state_baselines` test, so failures
and filters identify the owning aggregate module. It validates declarations,
checks all captured metadata, and reads all saved states for that module. Shared
checks exercise ordinary evaluation, serialization, old/new `_merge`,
`_merge_state` followed by reading, ordinary serialized merging used by
compaction, in-memory merging, and saved/new state combinations where declared.
These are aggregate-level checks, not a full materialized-view integration test.

For example, filter by `aggregates::sum::test_state_baselines` to check sum.
Metadata checks alone do not establish payload compatibility.
