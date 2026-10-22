# External JSON decoding

`FieldJsonAstDecoder` converts JSON DOM values directly into typed column builders.
The `json::JsonValue` interface is monomorphized for Serde and Sonic; arrays,
named tuples and maps use the same field-decoding implementation.

## Runtime-selectable Sonic NDJSON COPY parser

The standard binary includes both parsers; no special Cargo feature or release
artifact is needed. Serde remains the default. Enable Sonic for a session:

```sql
SET enable_sonic_json_parser = 1;
COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = NDJSON);
-- Immediate rollback for subsequent loads, without a restart:
SET enable_sonic_json_parser = 0;
```

The setting is captured in the input-format settings for a load, not fetched for
every row. It affects direct NDJSON COPY into typed columns, including VARIANT
columns. Stage `SELECT $1` (including COPY from a SELECT), SQL JSON functions and
other file formats are unchanged. There is no Databend architecture gate: Sonic
selects its SIMD implementation or portable fallback for the compilation target.
Performance on architectures other than x86_64/aarch64 must be measured rather
than assumed. Sonic 0.3.17 selects its main SIMD backend at **compile time** using
`target_feature`; this is not runtime CPU detection. A build with AVX2 enabled
must not be deployed to CPUs lacking AVX2, even if the session setting is off.

Use the deployment's normal CPU baseline. Do not add `target-cpu=native` to
portable release artifacts: a binary built for the build host may use unsupported
instructions on a different host. The existing Linux release build enables
SSE4.2; aarch64 provides NEON. Host-native optimization can be measured separately
when the deployment CPU is known. Cross-platform compilation does not replace
running correctness tests on both x86_64 and aarch64. The unchanged x86_64
SSE4.2 release flags do not enable AVX2. Sonic's README recommends native flags
for performance; they are not required for baseline-correct parsing.

`sonic-rs/arbitrary_precision` is a Cargo feature and is unified with other
consumers, independently of this runtime setting. The existing volo-thrift
consumer uses typed string maps, covered by a compatibility test. Switching the
setting off does not undo dependency-feature activation, the shared generic field
decoder, or binary size/build-time changes.

### Compatibility boundaries

- Both DOMs retain arbitrary-precision number text. DECIMAL decoding, including
  nested DECIMALs, reads that text rather than a rounded `f64` (#20748). It retains
  the sign-directed native integer fast path from #20788 and honors
  `numeric_cast_option`, including rounding carry/precision checks and bounded
  signed-i32 exponents. The shared decimal parser itself is not changed here.
- Float conversion follows the existing finite-f64 policy. Serde's normalization
  of integer `-0`, and its preservation of floating `-0.0`, must not be conflated.
  NUMBER-to-STRING uses Serde's number normalization, including exponent spelling.
- Object member names are expected to be unique. Duplicate keys are not checked
  or normalized, and do not trigger fallback. Their lookup/iteration behavior is
  backend-dependent; no first-wins/last-wins compatibility is promised. Typed
  tuple/map constraints still apply, so validation may also differ on such input.
- The reserved `$serde_json::private::Number` key still requires Serde behavior.
  A byte prefilter avoids inspecting the DOM for this key on ordinary rows;
  escaped spellings are checked on the decoded keys.
- A pre-parse depth check prevents Sonic's recursive DOM parser from exhausting
  the stack. Rows reaching 32 container levels use Serde, which retains its
  existing depth limit. Wide, shallow rows are not rejected by the depth check.
- Sonic parse failures and trailing input use the existing Serde error path;
  no unchecked UTF-8 or unchecked JSON parser is used. Field-decode diagnostics
  use Serde's canonical number spelling on the error path.
- Missing/null fields, `NULL_IF`, case matching, error handling and schema
  evolution remain in the shared row-decoding path.

### Wide-object lookup

`JsonObjectLookup` is shared by COPY rows and named tuples. COPY's existing
case-insensitive row index is retained; Serde's native exact-key index is reused.
For otherwise linear lookups, objects with at least 16 fields and at least eight
requested fields build one temporary hash index. Small objects and narrow
projections keep direct lookup/scan paths, avoiding index allocation.

For indexed objects the expected work is O(object fields + requested fields),
rather than their product. Exact-match keys borrow the DOM; folded keys are
normalized once per object. Values are always borrowed, not cloned. The index
costs O(object fields) temporary memory and is released after the row/tuple.
COPY's last case-folded match and tuples' first case-folded match are retained,
including distinct Unicode/case keys that normalize to the same name.

### Direct VARIANT conversion

Sonic VARIANT subtrees are converted **directly** to `jsonb::Value` and encoded
into the column builder. There is no intermediate JSON text or Serde DOM.
Strings borrow the Sonic arena during encoding; arrays allocate jsonb element
vectors and objects allocate its sorted map/owned keys. This removes the text
round trip, but is not allocation-free and still builds a jsonb value tree.

The bridge intentionally matches jsonb's existing `From<&serde_json::Value>`
policy, not jsonb's standalone text parser:

- Native nonnegative/signed integers use UInt64/Int64.
- Wider integers fitting i128/u128 use Decimal128/256, including the 38-digit
  Decimal128 coefficient boundary.
- Fractional/exponent numbers, and integers beyond those bounds, use finite f64.
  Numbers that cannot produce a finite f64 become null, as in the existing path.

This does **not** introduce exact fractional VARIANT storage or change existing
VARIANT precision. Encoded bytes are compared with the Serde path for numeric
boundaries, negative zero, subnormal/overflow values, escaping and generated
nested documents. Additional tests move strings/numbers across 16/32/64-byte
boundaries, vary slice alignment, truncate input at every byte, and reject invalid
Unicode/escapes/UTF-8 near block ends. These tests must run when upgrading the pinned jsonb dependency.

### Where to expect a benefit

The best candidates are CPU-bound direct NDJSON loads with long strings, many
numeric fields, or large VARIANT subtrees with strings/nested containers. Sonic's
arena reduces per-value allocations, typed fields read it directly, and VARIANT
conversion borrows string contents rather than cloning them through Serde.
No special duplicate-key detection or DOM deduplication is performed.

Expect smaller or negative gains for tiny rows, frequent invalid-input fallback,
deep documents, and loads dominated by object-store I/O, decompression,
expression evaluation or storage writes. Fallback after a Sonic parse failure
can parse a row twice. Do not infer COPY throughput from a parser-only benchmark;
measure peak memory and parallel loads as well.

### Validation and measurements

```sh
cargo test -p databend-common-formats --lib
cargo test -p databend-common-storages-stage --lib
cargo nextest run -p databend-common-settings
cargo clippy -p databend-common-formats -p databend-common-storages-stage \
  -p databend-common-settings --all-targets -- -D warnings
cargo run --release -p databend-common-formats --example ndjson_parser
cargo run --release -p databend-common-formats --example ndjson_parser -- --variant
cargo run --release -p databend-common-formats --example json_object_lookup
# Either mode accepts an NDJSON file path for a representative workload.
```

The example includes validation/compatibility checks, DOM destruction and Serde
fallback. `--variant` also includes direct conversion and jsonb encoding, reusing
an output buffer like a column builder. Neither mode includes decompression,
network I/O or table writes. `json_object_lookup` separately measures index
construction, field lookup and destruction, excluding parsing. Its row-mode
baseline includes the existing case-folded COPY map, rather than assuming COPY
always scanned linearly. It covers full tuples and narrow projections at several
widths and both case modes. Both parsers retain arbitrary-precision tokens, so
the baseline has the same DECIMAL correctness guarantee. Synthetic rows are
repeated in cache; measure real files and end-to-end COPY separately.

Run `tests/sqllogictests/suites/stage/formats/ndjson/`, following
`tests/sqllogictests/README.md` for stage preparation. The files' existing
`# run-with-settings: enable_sonic_json_parser=1` matrix directives run them
with the default Serde path and an additional Sonic pass.
`ndjson_decimal.test` covers #20748/#20788, `ndjson_typed_parser.test` covers
typed/nested values, `ndjson_wide_object.test` covers out-of-order wide rows and
named tuples, and `ndjson_variant_parser.test` switches
between the two parsers in one session to compare VARIANT/nested results and
exercise live rollback. Existing null/missing, on-error, stage SELECT and schema
evolution tests cover the shared behaviors.
