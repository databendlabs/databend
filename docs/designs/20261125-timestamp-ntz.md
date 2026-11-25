# TIMESTAMP_NTZ: A Wall-Clock Timestamp Type

This document proposes adding a timestamp type that carries no timezone, and
freezing the semantics of the existing timestamp types. It is a design
proposal: **no code changes are included with it.** It is written to be
reviewed on its own, before any implementation work starts.

## Summary

Databend has two timestamp types and both of them denote an instant:

| Type | Physical | Text in/out | Comparison |
|---|---|---|---|
| `TIMESTAMP` | `i64` micros, UTC instant | uses session `timezone` | by instant |
| `TIMESTAMP_TZ` | micros + offset | keeps the offset | by instant |

`TIMESTAMP` therefore already behaves as `TIMESTAMP WITH LOCAL TIME ZONE`
(SQL:2016), not as `TIMESTAMP`. Oracle's three-type family is therefore one
member short: the plain, timezone-free one.

This proposal adds that member as `TIMESTAMP_NTZ`, keeps `TIMESTAMP` exactly as
it is, and makes the family explicit by accepting the dialect spellings
`TIMESTAMP_LTZ`, `TIMESTAMP WITHOUT TIME ZONE`, and `TIMESTAMP WITH LOCAL TIME
ZONE`.

## Motivation

### 1. External formats have a naive timestamp that we currently misread

Parquet and Iceberg both distinguish `timestamp` (naive, wall clock) from
`timestamptz` (instant). Databend collapses both into `TIMESTAMP`:

```rust
// src/query/expression/src/converts/arrow/from.rs:156
ArrowDataType::Timestamp(_, _) => TableDataType::Timestamp,
```

The `_` discards both the timezone string and `isAdjustedToUTC`. Two
consequences follow:

- A naive Parquet/Iceberg `timestamp` column holds a wall-clock number that we
  reinterpret as a UTC instant, so reading it under `set timezone =
  'Asia/Shanghai'` shifts every value by 8 hours.
- Databend writes `TIMESTAMP` as `ArrowDataType::Timestamp(Microsecond, None)`
  (`src/query/expression/src/converts/arrow/to.rs:203`), which lands on disk as
  `TIMESTAMP(isAdjustedToUTC = false)` while containing UTC instants. Other
  engines then read Databend's columns as wall clock and see wrong values.

The Iceberg path has the same collapse because the schema goes through Arrow
(`src/query/storages/iceberg/src/table.rs:177-193`).

This is not a missing feature; it is an existing correctness bug with no clean
fix that does not introduce a naive type.

### 2. Wall-clock facts cannot be represented

Business hours, shift schedules, billing-period boundaries, flight and booking
times, "the 3rd of the month at 09:00" schedules, birthdays, audit records of
local time, and deadlines that must not move across a DST transition are all
wall-clock facts. Storing them as `TIMESTAMP` means the value's meaning depends
on the session timezone of whoever reads it, and requires resolving
non-existent or ambiguous local times at write time.

### 3. Round-trip determinism

`TIMESTAMP` text output depends on a session variable, which is why the SQL
logic tests are full of `set timezone = 'UTC'`. A wall-clock type renders
identically for every session, which makes exports, dashboards, and test
diffs reproducible.

### Why an `i64` column is not a substitute

The physical representation of `TIMESTAMP_NTZ` is the same `i64`, so it is fair
to ask what the type buys. It buys exactly the four things an integer column
cannot do:

- There is no timezone-free parser or formatter today; every text conversion
  takes a `&Tz` (`src/query/expression/src/types/timestamp.rs:204` and `:227`).
  An `i64` column therefore forces users through `to_unix_timestamp()`, which
  consults the session timezone — the very dependency being removed.
- Comparison against a literal, `min`/`max`, and range-index bounds can be
  typed as time instead of an unreadable integer.
- `extract`, `date_trunc`, `date_diff`, and interval arithmetic exist for the
  timestamp family and would have to be re-derived in integer arithmetic.
- Misuse is diagnosable: mixing an instant with a wall clock becomes an error
  at bind time instead of silent nonsense.

Ordering and equality are *not* part of the argument: for those two operations
an `i64` is genuinely equivalent, and the implementation relies on exactly
that.

### Non-goals

- Changing the semantics of `TIMESTAMP`. It stays an instant; there is no data
  migration and no re-interpretation of existing columns.
- Changing the on-disk encoding of existing columns.
- Reworking the `TIMESTAMP_TZ` encoding (`Decimal128(38, 0)` plus
  `ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE`). It is odd but it works and it is
  already in the wild.
- Implicit casts between the three timestamp types.
- Sub-day precision options, calendars other than proleptic Gregorian, or
  timezone-offset storage for `TIMESTAMP_NTZ`.

## Semantics

| Type | Stores | Text in/out | Ordering / equality | One-line meaning |
|---|---|---|---|---|
| `TIMESTAMP` (= `TIMESTAMP_LTZ`) | micros, UTC instant | session `timezone` | by instant | unchanged |
| `TIMESTAMP_TZ` | micros + offset | keeps offset | by instant | unchanged |
| `TIMESTAMP_NTZ` | micros, civil | never consults session `timezone` | numeric over civil micros | **new** |

### Invariants

- **I1 — Timezone isolation.** Changing `set timezone` to any value does not
  change the text input or output of a `TIMESTAMP_NTZ` column.
- **I2 — No implicit conversion.** There is no implicit cast between
  `TIMESTAMP_NTZ` and `TIMESTAMP`/`TIMESTAMP_TZ` in either direction.
  Comparisons and binary operations that mix them are bind-time errors that
  name the explicit function to use.
- **I3 — Physical identity.** The encoding of `TIMESTAMP_NTZ` is byte-for-byte
  identical to `TIMESTAMP`. Compression, dictionary encoding, row encoding,
  `min`/`max` statistics, cluster keys, and aggregate state encodings are
  reused unchanged.

### Computation uses the civil calendar, not UTC

`TIMESTAMP_NTZ` arithmetic is defined on the civil calendar: the `i64` is
microseconds since `1970-01-01 00:00:00` *of the civil calendar*, with no
offset applied.

Implementation-wise this is expressed by binding the existing
timezone-parameterised kernels to `Tz::UTC`. That is an implementation
shortcut, not the definition, and it is only valid because UTC has no DST and a
constant zero offset, which makes

```text
components_from_timestamp(v, &Tz::UTC) == civil fields of v
```

hold for every value. The shortcut must not be confused with the semantics in
the following places:

1. **Format specifiers.** `%z` and `%Z` must be rejected for `TIMESTAMP_NTZ`,
   not rendered as `+0000` / `UTC`. Printing an offset would claim an offset
   the value does not have.
2. **Range bounds.** `TIMESTAMP_MIN` is `0000-12-31 00:00:00` for a reason that
   only applies to instants (`src/query/expression/src/types/timestamp.rs:44`:
   one extra UTC day preserves local year-1 midnight after timezone
   resolution). `TIMESTAMP_NTZ` must clamp on its own civil bound,
   `0001-01-01 00:00:00` .. `9999-12-31 23:59:59.999999`. The upper bound is
   the same as `TIMESTAMP_MAX`.
3. **Epoch interop.** `to_int64` / `to_timestamp_ntz(int64)` remain available,
   but the document must say the integer uses UTC epoch numbering as a
   convention, not that the value is in UTC. `DATE` has exactly the same
   property today and nobody calls `DATE` a UTC type.
4. **Instants.** `now()` / `today()` are instants and do not enter
   `TIMESTAMP_NTZ` implicitly. The error message for the common mistake must
   show the full replacement, e.g. `to_timestamp_ntz(now(), 'Asia/Shanghai')`.
5. **Non-timezone settings still apply.** `FuncContext` carries `week_start`
   (`src/query/expression/src/function.rs:157`) in addition to `tz`. "Does not
   read the timezone" does not mean "does not read any setting": `week()`,
   `dayofweek()`, `%U`, and `%V` must still honour `week_start`.

For cross-type conversions the timezone is the one supplied by the caller:

| Operation | Timezone used |
|---|---|
| `TIMESTAMP_NTZ` field extraction, truncation, calendar arithmetic, compare, sort | civil calendar (`Tz::UTC` binding) |
| `to_timestamp_ntz(<instant>, <tz>)`, `to_timestamp_ltz(<ntz>, <tz>)` | the caller's `<tz>` |
| `now()`, `today()`, `convert_timezone`, `to_timezone`, `date_trunc(x, tz)` | rejected for `TIMESTAMP_NTZ` |

### Text format

- Output: `%Y-%m-%d %H:%M:%S%.6f`, no offset, identical to `TIMESTAMP` rendered
  under a UTC session.
- Input: an absent offset, or a zero offset (`Z`, `+00:00`) is accepted. A
  non-zero offset is rejected with an error that suggests `to_timestamp()`.
  Silently dropping an offset is the exact failure mode this type exists to
  remove.

### Interval arithmetic

Reuses the timestamp kernels bound to `Tz::UTC`
(`src/query/functions/src/scalars/timestamp/src/date_arithmetic.rs:146-150`
already takes `tz` as a parameter). Because there is no DST,
`+ INTERVAL 1 DAY` and `+ INTERVAL 24 HOUR` coincide, `+ INTERVAL 1 MONTH`
carries on civil fields with day-of-month clamping, and results are
unambiguous.

### TTL

`TIMESTAMP_NTZ` is rejected as a TTL result type. TTL decides when to drop
data, which requires an instant; the current planner casts the TTL column to
`TypeName::Timestamp` (`src/query/sql/src/planner/expression/ttl.rs:186`), and
invariant I2 turns that into an error, which needs a dedicated message.

## Syntax

```sql
CREATE TABLE t (
    a TIMESTAMP,                          -- extends as today: instant, session tz
    b TIMESTAMP_LTZ,                      -- alias of a
    c TIMESTAMP_NTZ,                      -- wall clock
    d TIMESTAMP_TZ                        -- instant + stored offset
);

CREATE TABLE t (a TIMESTAMP WITHOUT TIME ZONE);      -- = TIMESTAMP_NTZ
CREATE TABLE t (a TIMESTAMP WITH LOCAL TIME ZONE);   -- = TIMESTAMP

SELECT TIMESTAMP_NTZ '2024-03-10 02:30:00', TIMESTAMP_LTZ '2024-03-10 02:30:00';
```

Parser notes:

- `TIMESTAMP_TZ` is a single lexer token, not `TIMESTAMP` `TZ`
  (`src/query/ast/src/parser/token.rs:1363`). `TIMESTAMP_NTZ` and
  `TIMESTAMP_LTZ` follow the same pattern. `WITHOUT` needs a new token; `WITH`,
  `TIME`, and `ZONE` already exist.
- The `ty_datetime` rule consumes a bare `TIMESTAMP` before anything else
  (`src/query/ast/src/parser/expr.rs:2359`), so the new rules must be ordered
  before it, exactly like `ty_timestamp_tz` is today
  (`src/query/ast/src/parser/expr.rs:2408-2409`).
- `TIMESTAMP_NTZ(p)` with a precision argument is rejected; the type has no
  precision parameter.

Normalization: `TIMESTAMP_LTZ`, `TIMESTAMP WITH LOCAL TIME ZONE` all resolve to
`TypeName::Timestamp`, so `SHOW CREATE TABLE`, `DESC`, and
`information_schema.columns` keep reporting `TIMESTAMP`. Deliberately no second
schema representation for the same type.

Display: the new type follows the existing `Timestamp_Tz` / `TIMESTAMP_TZ`
convention (`src/query/expression/src/types.rs:720` and `:742`), i.e.
`Timestamp_Ntz` / `TIMESTAMP_NTZ`.

## Type system landing points

| Layer | Location | Change |
|---|---|---|
| AST | `TypeName` (`src/query/ast/src/ast/expr.rs:1224`), parser, `Display` | add `TimestampNtz`; **no** `TimestampLtz` variant |
| AST → schema | `src/query/expression/src/types/type_name.rs:54` | new arm |
| Table schema | `TableDataType::TimestampNtz` (`src/query/expression/src/schema.rs:346`) | new variant; the only new meta identity |
| Expression | `DataType`, `Scalar`, `ScalarRef`, `Column`, `ColumnBuilder`, `Value`, `BlockEntry`, `Domain` | new variants, physical type `timestamp` (`i64`), modelled on `types/timestamp.rs` |
| Meta | `proto-conv`: new `TableDataType` variant, conv version +1 (`src/meta/proto-conv/src/util.rs:189` is the v157 precedent) | old nodes reject via version, they cannot misread |
| Arrow / Parquet | `converts/arrow/to.rs`, `from.rs` | `Timestamp(us, None)` **plus** field metadata `ARROW_EXT_TYPE_TIMESTAMP_NTZ` |

The last row is the non-obvious one. `from.rs:156` currently turns every Arrow
timestamp into `TableDataType::Timestamp`, so a `TIMESTAMP_NTZ` column written
and read back would silently become `TIMESTAMP` unless the arrow type carries
an identity marker. `TIMESTAMP_TZ` already relies on that mechanism with
`ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE` (`from.rs:110`); field metadata survives a
round trip inside the `ARROW:schema` blob, which that type proves in practice.

Reading rule that follows from it, and that must not be broken:

> An Arrow `Timestamp(_, None)` **without** an extension key is `TIMESTAMP`
> (LTZ), forever, because that is what existing files contain.

Consequence: a foreign Parquet file with a naive timestamp is still typed as
`TIMESTAMP`. Fixing that belongs to the external-table boundary, where
`isAdjustedToUTC` is available and the schema is inferred explicitly (phase
P3), not to the generic reader.

## Casts and functions

### Casts

| From → To | String | Date | Int64 | `TIMESTAMP` | `TIMESTAMP_TZ` | `TIMESTAMP_NTZ` |
|---|---|---|---|---|---|---|
| `TIMESTAMP_NTZ` | yes, no tz | yes, civil date | yes, micros | no | no | — |
| `TIMESTAMP` → `TIMESTAMP_NTZ` | — | — | — | — | — | explicit only |
| `TIMESTAMP_NTZ` → `TIMESTAMP` | — | — | — | explicit only | explicit only | — |

Explicit conversions:

- `to_timestamp_ntz(<string>)`, `to_timestamp_ntz(<string>, <format>)`,
  `try_to_timestamp_ntz(...)` — mirroring the one- and two-argument forms of
  `to_timestamp`.
- `to_timestamp_ntz(<instant>, <tz>)` — instant → wall clock.
- `to_timestamp_ltz(<ntz>, <tz>)`, `to_timestamp_tz(<ntz>, <tz>)` — wall clock →
  instant. Names follow the existing cast-function convention
  (`src/query/expression/src/type_check.rs:995`), no new vocabulary.
- Ambiguous or non-existent local times in the explicit conversions reuse the
  existing `databend_common_timezone` resolution policy. No new behaviour.

### Functions in v1

Everything below runs on the existing kernels with the timezone bound to UTC:
`to_string` / `date_format`, `extract` / `date_part`, `date_trunc`, interval
add and subtract, `to_date`, `to_int64` / `to_uint64`, `min`, `max`, `any`,
`arg_min`, `arg_max`, `count`, `count_distinct`, `GROUP BY`, `ORDER BY`,
`DISTINCT`, hashing, `coalesce`, `if`, `case`, `nullif`, window partitioning.

Explicitly rejected in v1, with an error naming the alternative: any function
with a timezone parameter (`convert_timezone`, `to_timezone`,
`date_trunc(x, tz)`, `time_slot`), comparison with `now()` / `today()`,
`TIMESTAMP_NTZ` inside `VARIANT` (string form only), and `TIMESTAMP_NTZ` in a
TTL expression.

## Compatibility and rollout

- **Existing tables:** schemas, storage format, and semantics unchanged. No
  migration, no backfill, no rewrite.
- **New meta:** `TIMESTAMP_NTZ` columns need the conv version bump, so during a
  rolling upgrade a new node can create a table that old query nodes refuse to
  plan. That is the intended failure mode — an error, not a misread.
- **Aliases:** `TIMESTAMP_LTZ` and the two standard spellings are parse-time
  only and never persisted, so they cannot create a schema difference.
- **Gate:** optional `enable_timestamp_ntz` setting, default off while the type
  matures, so earlier phases can land without user exposure.

### Phases

- **P0 — shrink the future cost (no new type, can land first).** Fold the
  `matches!(x, DataType::Timestamp | DataType::TimestampTz)` patterns that
  silently exclude new members into `DataType::is_timestamp_family()` /
  `as_timestamp_family()` helpers. There are 289 `DataType::Timestamp` sites in
  the tree; exhaustive `match` arms are caught by the compiler, `matches!`
  predicates are not. This work is worth doing whether or not the rest ships.
- **P1 — store and fetch.** AST, parser, typed literal, `DataType` /
  `TableDataType`, proto version bump, Arrow extension key, display and parse,
  DDL, insert/select round trip. At this point `TIMESTAMP_NTZ` supports only
  `to_string`, comparison, and `ORDER BY`.
- **P2 — usable.** The cast matrix above, the v1 function set, and the dialect
  aliases (`TIMESTAMP_LTZ`, `WITHOUT TIME ZONE`, `WITH LOCAL TIME ZONE`).
- **P3 — interop.** Type external Parquet `isAdjustedToUTC = false` and Iceberg
  `timestamp` as `TIMESTAMP_NTZ`, Iceberg `timestamptz` as `TIMESTAMP_TZ` (which
  also stops `table.rs:177` from dropping the offset). This is the phase that
  pays back the bug described in Motivation §1.
- **P4 — documentation.** Semantics table plus a "which timestamp type should I
  use" guide.

## Testing

1. `proto-conv` compatibility test, following `v157_type_timestamp_tz.rs`.
2. `TIMESTAMP_NTZ` round-trip SQL logic tests; **every case asserted under
   `UTC`, `Asia/Shanghai`, and `America/New_York`** in the same run (I1). This
   is the only reliable way to catch a leaked `ctx.func_ctx.tz`.
3. Negative tests: `ntz = ltz`, `ntz = now()`, `ltz::timestamp_ntz`, TTL on
   `TIMESTAMP_NTZ`, `%z` / `%Z` on `TIMESTAMP_NTZ`, `TIMESTAMP_NTZ(6)`.
4. `SHOW CREATE TABLE`, `DESC`, and `information_schema.columns.data_type`
   output, including that the aliases normalize to `TIMESTAMP`.
5. Parser ambiguity cases: `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP WITH TIME
   ZONE`, `TIMESTAMP(6)`.
6. P3 interop: read an `isAdjustedToUTC = false` Parquet file under a non-UTC
   session and assert the wall clock is unchanged.

## Open questions

1. **Offset-bearing input.** The proposal rejects a non-zero offset and accepts
   a zero one. The alternative — Snowflake's behaviour of ignoring the offset —
   is friendlier but reintroduces a silent-conversion class of bug.
2. **Setting gate.** Whether `enable_timestamp_ntz` is worth having, versus
   landing P1 behind a compile-time constant.
3. **LTZ on-disk encoding.** Should `TIMESTAMP` be written as
   `Timestamp(us, Some("UTC"))` so that it is honest on disk and naturally
   distinct from `TIMESTAMP_NTZ`? It is the cleaner long-term encoding but it
   changes the logical annotation of new files written for existing tables, and
   it changes what external engines see. Suggested as a separate proposal, not
   part of this one.
4. **Cost expectation.** Adding `TimestampTz` touched 84 files and about 2.2k
   lines and kept producing boundary fixes for months afterwards. `TimestampNtz`
   should be cheaper per site because the physical type is reused and there is
   no offset field, but P0 is what makes that estimate hold.
