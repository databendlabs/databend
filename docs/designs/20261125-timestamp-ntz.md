# TIMESTAMP_NTZ: A Wall-Clock Timestamp Type, Aligned with Snowflake

This document proposes adding a timestamp type that carries no timezone, and
freezing the semantics of the existing timestamp types. It is a design
proposal: **no code changes are included with it.** The target is behavioural
compatibility with Snowflake's four timestamp spellings, on top of what
Databend already implements.

## Summary

Databend has two timestamp types and both of them denote an instant:

| Type | Physical | Text in/out | Comparison |
|---|---|---|---|
| `TIMESTAMP` | `i64` micros, UTC instant | uses session `timezone` | by instant |
| `TIMESTAMP_TZ` | micros + offset | keeps the offset | by instant |

`TIMESTAMP` therefore already behaves as `TIMESTAMP WITH LOCAL TIME ZONE`
(SQL:2016), not as `TIMESTAMP`. Snowflake's family is one member short here: the
plain, timezone-free type.

This proposal adds it as `TIMESTAMP_NTZ`, keeps `TIMESTAMP` exactly as it is,
and aligns the naming, aliasing, and conversion rules with Snowflake:

1. New logical type `TIMESTAMP_NTZ` whose physical encoding is byte-identical to
   `TIMESTAMP`.
2. Dialect aliases that cost nothing to parse: `TIMESTAMP_LTZ`,
   `TIMESTAMP_NTZ`, `TIMESTAMPLTZ`, `TIMESTAMPNTZ`, `TIMESTAMPTZ`,
   `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP WITH LOCAL TIME ZONE`,
   `TIMESTAMP WITH TIME ZONE`, `DATETIME`.
3. A session parameter `timestamp_type_mapping` that resolves the bare
   `TIMESTAMP` and `DATETIME` names to one of the three concrete types, as
   Snowflake's parameter of the same name does. Databend default: `LTZ`, so
   nothing changes for existing users.
4. Implicit conversion among the three timestamp types, plus the two explicit
   forms Snowflake users expect: `CAST(x AS TIMESTAMP_NTZ)` and
   `to_timestamp_ntz(x, '<tz>')`. The session timezone is the bridge between
   wall clock and instant; a `TIMESTAMP_TZ` operand instead converts with the
   offset it already carries.

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
on the session timezone of whoever reads it.

### 3. Round-trip determinism

`TIMESTAMP` text output depends on a session variable, which is why the SQL
logic tests are full of `set timezone = 'UTC'`. A wall-clock type renders
identically for every session, which makes exports, dashboards, and test diffs
reproducible — as long as the expression does not mix timestamp types, see I1.

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
- The three-way conversion matrix can be defined once, instead of being
  re-invented per query.

Ordering and equality are *not* part of the argument: for those two operations
an `i64` is genuinely equivalent, and the implementation relies on exactly
that.

### Non-goals

- Changing the semantics of `TIMESTAMP` itself, or re-interpreting existing
  columns. What changes is how the *name* `TIMESTAMP` resolves, and only when a
  non-default `timestamp_type_mapping` is set.
- Changing the on-disk encoding of existing columns.
- Reworking the `TIMESTAMP_TZ` encoding (`Decimal128(38, 0)` plus
  `ARROW_EXT_TYPE_TIMESTAMP_TIMEZONE`). It is odd but it works and it is
  already in the wild.
- Moving to nanosecond precision (Databend stays at microseconds), calendars
  other than proleptic Gregorian, or storing a timezone name (rather than an
  offset) in `TIMESTAMP_TZ`.

## Snowflake compatibility

Verified against Snowflake's data type documentation. After this proposal, the
four spellings line up as follows.

| Snowflake | Snowflake semantics | Databend today | After this proposal |
|---|---|---|---|
| `TIMESTAMP` | alias resolved by `TIMESTAMP_TYPE_MAPPING`, default `NTZ`; never stored | fixed `LTZ` | resolved by `timestamp_type_mapping`, default `LTZ` |
| `TIMESTAMP_NTZ` (`TIMESTAMPNTZ`, `TIMESTAMP WITHOUT TIME ZONE`, `DATETIME`) | stores wall clock, "all operations are performed without taking any time zone into account" | missing | `TIMESTAMP_NTZ` |
| `TIMESTAMP_LTZ` (`TIMESTAMPLTZ`, `TIMESTAMP WITH LOCAL TIME ZONE`) | stores UTC, operates in the session `TIMEZONE` | `TIMESTAMP` | alias of `TIMESTAMP` |
| `TIMESTAMP_TZ` (`TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE`) | stores UTC + offset, "compared based on their times in UTC" | `TIMESTAMP_TZ` | unchanged |

Two properties already match and must stay matched:

- **Comparison of `TIMESTAMP_TZ` is by UTC instant, ignoring the offset.**
  Snowflake: `'2024-01-01 00:00:00 +0000'::TIMESTAMP_TZ =
  '2024-01-01 01:00:00 +0100'::TIMESTAMP_TZ` is true. Databend asserts the same
  result in `tests/sqllogictests/suites/base/11_data_type/11_0001_data_type_date_time.test:119-121`,
  and the equality-grouping code says so explicitly in a comment at
  `src/query/expression/src/types.rs:655`.
- **A literal without an offset takes the session timezone.** Snowflake applies
  the session offset; Databend resolves through
  `timestamp_tz_from_local(local, tz)` (`src/query/expression/src/types/timestamp_tz.rs:234`).

Deliberate divergences, to be documented rather than fixed:

- **Precision stays at microseconds.** Databend does not move to nanoseconds.
  Snowflake's default is 9 digits; ours is 6, permanently. The DDL policy is
  that `TIMESTAMP(p)` / `DATETIME(p)` / `TIMESTAMP_NTZ(p)` with `p <= 6` parse
  and the argument is ignored, while `p` in 7..=9 is rejected with an explicit
  error rather than accepted and silently truncated. Failing once at
  `CREATE TABLE` is visible; truncating every subsequent insert is not. This is
  a one-line policy if we later prefer Snowflake's tolerance.
- `TIMESTAMP_NTZ` rejects `%z` / `%Z` in formatting. Snowflake prints `Z` when
  the output format contains a timezone field. Printing an offset for a value
  that has none is exactly the confusion this type exists to remove.

Decisions taken in this revision, at the cost of the previous "no implicit
conversion" rule:

- Bare `TIMESTAMP` and `DATETIME` are both resolved by
  `timestamp_type_mapping`; `DATETIME` is not hard-wired to `NTZ`.
- Both cast spellings are supported: `CAST(x AS TIMESTAMP_NTZ)` uses the
  session timezone, and `to_timestamp_ntz(x, '<tz>')` takes it explicitly.
- Implicit conversion between the three timestamp types is allowed.

## Semantics

| Type | Stores | Text in/out | Ordering / equality | One-line meaning |
|---|---|---|---|---|
| `TIMESTAMP` (= `TIMESTAMP_LTZ`) | micros, UTC instant | session `timezone` | by instant | unchanged |
| `TIMESTAMP_TZ` | micros + offset | keeps offset | by instant | unchanged |
| `TIMESTAMP_NTZ` | micros, civil | never consults session `timezone` | numeric over civil micros | **new** |

### Invariants

- **I1 — Timezone isolation for homogeneous expressions.** An expression in
  which every timestamp operand is `TIMESTAMP_NTZ` produces the same result
  under every session timezone. As soon as an `TIMESTAMP_NTZ` value meets an
  `TIMESTAMP` or `TIMESTAMP_TZ` value, the session timezone applies — by
  design, and by Snowflake's rules.
- **I2 — One conversion implementation.** Every conversion between the three
  timestamp types goes through a single set of rules (the coercion table in
  "Conversion model"). No ad-hoc `match` on the timestamp family may convert
  types on its own.
- **I3 — Physical identity.** The encoding of `TIMESTAMP_NTZ` is byte-for-byte
  identical to `TIMESTAMP`. Compression, dictionary encoding, row encoding,
  `min`/`max` statistics, cluster keys, and aggregate state encodings are
  reused unchanged.
- **I4 — Native signatures for supported functions.** Every function that
  claims `TIMESTAMP_NTZ` support in the v1 matrix has a real
  `TIMESTAMP_NTZ` signature. It must not fall back to implicit conversion into
  an instant-based signature, because that would silently reintroduce the
  session timezone (see "Where the session timezone enters").

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

1. **Format specifiers.** `%z` and `%Z` are rejected for `TIMESTAMP_NTZ`, not
   rendered as `+0000` / `UTC`.
2. **Range bounds.** `TIMESTAMP_MIN` is `0000-12-31 00:00:00` for a reason that
   only applies to instants (`src/query/expression/src/types/timestamp.rs:44`:
   one extra UTC day preserves local year-1 midnight after timezone
   resolution). `TIMESTAMP_NTZ` clamps on its own civil bound,
   `0001-01-01 00:00:00` .. `9999-12-31 23:59:59.999999`, which is also
   Snowflake's practical range. The upper bound equals `TIMESTAMP_MAX`.
3. **Epoch interop.** `to_int64` / `to_timestamp_ntz(int64)` remain available,
   but the integer uses UTC epoch numbering as a convention, not as a claim
   that the value is in UTC. `DATE` has exactly the same property today.
4. **Non-timezone settings still apply.** `FuncContext` carries `week_start`
   (`src/query/expression/src/function.rs:157`) in addition to `tz`. "Does not
   read the timezone" does not mean "does not read any setting": `week()`,
   `dayofweek()`, `%U`, and `%V` still honour `week_start`.

### Text format

- Output: `%Y-%m-%d %H:%M:%S%.6f`, no offset, identical to `TIMESTAMP`
  rendered under a UTC session.
- Input: an absent offset, or a zero offset (`Z`, `+00:00`) is accepted. A
  non-zero offset is rejected with an error that names the alternative
  spellings. Silently dropping an offset is the failure mode this type exists
  to remove.

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
with `timestamp_type_mapping` set to `NTZ` that cast would silently become
lossy. TTL resolves its type through a dedicated instant coercion instead.

## Conversion model

### Implicit conversions

Implicit conversion is allowed among all three types. The result type of a
mix is the most information-rich operand:

```text
TIMESTAMP_TZ  >  TIMESTAMP (LTZ)  >  TIMESTAMP_NTZ
```

Rationale: converting towards the richer type never discards the instant. It
also means an implicit conversion *never produces* `TIMESTAMP_NTZ`; wall clock
values are only created by explicit casts, by `timestamp_type_mapping`, or by a
table column type.

| Context | Result / target type |
|---|---|
| Comparison, `BETWEEN`, `IN`, `CASE`, `COALESCE`, `UNION` | richest operand (`TZ` > `LTZ` > `NTZ`) |
| Join key, `GROUP BY`, `DISTINCT` | each side converted to the richest operand before hashing |
| Function argument | the signature that is chosen by normal overload resolution; I4 requires a native `NTZ` signature for the v1 matrix |
| `INSERT` / `UPDATE` / assignment | the target column type |
| String literal against a timestamp column | the column type (so a string literal next to `NTZ` is parsed without a timezone) |

### Explicit conversions

Both forms are supported. They differ only in where the timezone comes from:

| Form | Timezone |
|---|---|
| `CAST(x AS TIMESTAMP_NTZ)`, `x::TIMESTAMP_NTZ` | implied: session `TIMEZONE` for an `LTZ` operand, the value's own stored offset for a `TZ` operand, none at all for a string operand |
| `to_timestamp_ntz(x, '<tz>')`, `try_to_timestamp_ntz(...)` | the given `<tz>`, overriding any stored offset |
| `CAST(x AS TIMESTAMP)`, `x::TIMESTAMP` | the same implied timezone, then the result is subject to `timestamp_type_mapping` |
| `to_timestamp_ltz(x, '<tz>')`, `to_timestamp_tz(x, '<tz>')` | the given `<tz>` |

So `CAST` never asks the caller for a timezone, and the two-argument form always
overrides. This keeps Snowflake's ergonomics (`x::TIMESTAMP_NTZ` just works)
while still letting a caller be explicit when the session timezone is not the
right answer:

```sql
SET timezone = 'Asia/Shanghai';
SELECT '2024-01-01 12:00:00 -0800'::TIMESTAMP_TZ::TIMESTAMP_NTZ;              -- 2024-01-01 12:00:00
SELECT to_timestamp_ntz('2024-01-01 12:00:00 -0800'::TIMESTAMP_TZ, 'UTC');   -- 2024-01-01 20:00:00
```

`to_timestamp_ntz` also keeps the one- and two-argument string forms
(`to_timestamp_ntz(<string>)`, `to_timestamp_ntz(<string>, <format>)`),
mirroring `to_timestamp`.

### Numeric effect of a conversion

| From → To | Micros | Offset |
|---|---|---|
| `NTZ` → `LTZ` | `resolve_local(civil, session_tz)` | — |
| `NTZ` → `TZ` | `resolve_local(civil, session_tz)` | set from the session offset |
| `LTZ` → `NTZ` | `render(instant, session_tz)` | — |
| `LTZ` → `TZ` | unchanged | set from the session offset |
| `TZ` → `LTZ` | unchanged | dropped |
| `TZ` → `NTZ` | `instant + stored offset` | dropped |

The `TZ` → `NTZ` row uses the value's **own stored offset**, not the session
timezone. A `TIMESTAMP_TZ` value already carries the offset it was created
with, so its civil representation is defined without asking the session
anything; converting through the session timezone would invent a wall clock the
value never had. The property this buys, and that the tests assert:

```sql
'2024-01-01 12:00:00 -0800'::TIMESTAMP_TZ::TIMESTAMP_NTZ  ==>  2024-01-01 12:00:00
```

under any session timezone. Note that the reverse direction is not symmetric —
`TIMESTAMP_NTZ → TIMESTAMP_TZ` has no offset to recover and therefore uses the
session timezone — which is exactly the information loss that makes this an
explicit cast rather than an implicit one.

### Where the session timezone enters

Once conversions are implicit, the session timezone participates in query
*planning*, not only in expression evaluation. These are the places that must
be audited, and each one gets a test:

- **Constant folding** of a literal next to an `NTZ` column. Folding must use
  the query's timezone, not the planner's default. There is precedent for
  getting this wrong: `fix(planner): honor query settings in folding and
  statistics (#20448)`.
- **Statistics and range-index pruning.** A predicate that mixes types must be
  converted to the column's type before min/max comparison; a half-converted
  bound prunes valid rows silently.
- **Plan cache.** A plan whose folded literals depend on `timestamp_type_mapping`
  or on `timezone` must not be reused under different settings.
- **`INSERT` / `UPDATE` / CTAS.** A stored value depends on the timezone of the
  statement that produced it whenever the source is an `LTZ` value being
  written into an `NTZ` column. Sources that are already `NTZ` or `TIMESTAMP_TZ`
  are session-independent, which is a reason to prefer keeping instants in
  `TIMESTAMP_TZ` when the original offset matters.
- **Prepared statements.** The resolved type of a bare `TIMESTAMP` in a
  prepared statement is fixed at prepare time; changing
  `timestamp_type_mapping` afterwards does not re-resolve it.

## Syntax

```sql
SET timestamp_type_mapping = 'NTZ';   -- 'LTZ' (default) | 'NTZ' | 'TZ'

CREATE TABLE t (
    a TIMESTAMP,                          -- resolved by timestamp_type_mapping
    b TIMESTAMP_LTZ,                      -- alias of TIMESTAMP
    c TIMESTAMP_NTZ,                      -- wall clock
    d TIMESTAMP_TZ,                       -- instant + stored offset
    e DATETIME                            -- same mapping as TIMESTAMP
);

CREATE TABLE t (a TIMESTAMP WITHOUT TIME ZONE);      -- = TIMESTAMP_NTZ
CREATE TABLE t (a TIMESTAMP WITH LOCAL TIME ZONE);   -- = TIMESTAMP
CREATE TABLE t (a TIMESTAMP WITH TIME ZONE);         -- = TIMESTAMP_TZ

SELECT TIMESTAMP_NTZ '2024-03-10 02:30:00', TIMESTAMP_LTZ '2024-03-10 02:30:00';
```

Aliases that resolve without consulting settings:

| Spelling | Resolves to |
|---|---|
| `TIMESTAMP_LTZ`, `TIMESTAMPLTZ`, `TIMESTAMP WITH LOCAL TIME ZONE` | `TypeName::Timestamp` |
| `TIMESTAMP_NTZ`, `TIMESTAMPNTZ`, `TIMESTAMP WITHOUT TIME ZONE` | `TypeName::TimestampNtz` |
| `TIMESTAMP_TZ`, `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `TypeName::TimestampTz` |

Aliases that consult `timestamp_type_mapping`:

| Spelling | Resolves to |
|---|---|
| `TIMESTAMP` | the mapped concrete type |
| `DATETIME` | the mapped concrete type |

Parser notes:

- `TIMESTAMP_TZ` is a single lexer token, not `TIMESTAMP` `TZ`
  (`src/query/ast/src/parser/token.rs:1363`). `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`,
  `TIMESTAMPNTZ`, `TIMESTAMPLTZ`, and `TIMESTAMPTZ` follow the same pattern.
  `WITHOUT` needs a new token; `WITH`, `TIME`, and `ZONE` already exist.
- The `ty_datetime` rule consumes a bare `TIMESTAMP` and `DATETIME` before
  anything else (`src/query/ast/src/parser/expr.rs:2359`), so the new rules must
  be ordered before it, exactly like `ty_timestamp_tz` is today
  (`src/query/ast/src/parser/expr.rs:2408-2409`).
- The precision argument `p` is accepted and ignored for all four type names
  when `p <= 6`, and rejected when `p` is 7..=9 (see "Deliberate divergences").
  All timestamp types are stored as microseconds regardless.

### Resolving a settings-dependent type name

Today the AST type name becomes a schema type in one place that has no access
to settings (`src/query/expression/src/types/type_name.rs:54`). A
`timestamp_type_mapping`-dependent `TIMESTAMP` cannot be resolved there. The
resolution point moves into the binder, which has the session settings, and the
low-level function keeps a parameter-free default (`LTZ`) for callers that have
no session (tests, meta replay, interop conversion). DDL resolves once and
stores the concrete type, which is why `SHOW CREATE TABLE` will always print
`TIMESTAMP_LTZ` / `TIMESTAMP_NTZ` / `TIMESTAMP_TZ` and never the alias.

## Type system landing points

| Layer | Location | Change |
|---|---|---|
| AST | `TypeName` (`src/query/ast/src/ast/expr.rs:1224`), parser, `Display` | add `TimestampNtz`; aliases and mapping resolution keep the four existing variants |
| AST → schema | `src/query/expression/src/types/type_name.rs:54` | new arm; plus the binder-side resolution described above |
| Table schema | `TableDataType::TimestampNtz` (`src/query/expression/src/schema.rs:346`) | new variant; the only new meta identity |
| Expression | `DataType`, `Scalar`, `ScalarRef`, `Column`, `ColumnBuilder`, `Value`, `BlockEntry`, `Domain` | new variants, physical type `timestamp` (`i64`), modelled on `types/timestamp.rs` |
| Coercion | type coercion rules | the precedence table, in one place (I2) |
| Settings | `src/query/settings` | `timestamp_type_mapping`, values `LTZ` / `NTZ` / `TZ`, default `LTZ` |
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

## Functions in v1

Everything below gets a native `TIMESTAMP_NTZ` signature (I4) and runs on the
existing kernels with the timezone bound to UTC: `to_string` / `date_format`,
`extract` / `date_part`, `date_trunc`, interval add and subtract, `to_date`,
`to_int64` / `to_uint64`, `min`, `max`, `any`, `arg_min`, `arg_max`, `count`,
`count_distinct`, `GROUP BY`, `ORDER BY`, `DISTINCT`, hashing, `coalesce`, `if`,
`case`, `nullif`, window partitioning.

Rejected in v1, with an error naming the alternative: functions that take a
timezone parameter together with an `NTZ` argument (`convert_timezone`,
`to_timezone`, `date_trunc(x, tz)`), and `TIMESTAMP_NTZ` inside `VARIANT`
(string form only). `TIMESTAMP_NTZ` in a TTL expression is rejected as
described above.

Note the change from the previous revision: `ntz_col < now()` is now *allowed*
and converts the column to `TIMESTAMP` in the session timezone. It is
Snowflake-compatible, and it is the clearest example of I1's boundary.

## Compatibility and rollout

- **Existing tables:** schemas, storage format, and semantics unchanged. No
  migration, no backfill, no rewrite. The default `timestamp_type_mapping` is
  `LTZ`, which reproduces today's meaning of bare `TIMESTAMP` and `DATETIME`
  exactly.
- **New meta:** `TIMESTAMP_NTZ` columns need the conv version bump, so during a
  rolling upgrade a new node can create a table that old query nodes refuse to
  plan. That is the intended failure mode — an error, not a misread.
- **Aliases:** parse-time only and never persisted, so they cannot create a
  schema difference.
- **Gate:** optional `enable_timestamp_ntz` setting, default off while the type
  matures, so earlier phases can land without user exposure. The mapping
  parameter and the implicit conversions are behind the same gate until P2.
- **Migration advice:** a Snowflake workload should run with
  `SET timestamp_type_mapping = 'NTZ'` for the session or user that executes
  its DDL, and should be told that changing the setting afterwards does not
  change already-created tables.

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
- **P2 — Snowflake-shaped behaviour.** The conversion model (implicit
  precedence, both explicit spellings, the numeric-effect table),
  `timestamp_type_mapping`, the `DATETIME` mapping, the v1 function set, and
  all the aliases.
- **P3 — interop.** Type external Parquet `isAdjustedToUTC = false` and Iceberg
  `timestamp` as `TIMESTAMP_NTZ`, Iceberg `timestamptz` as `TIMESTAMP_TZ`
  (which also stops `table.rs:177` from dropping the offset). This is the phase
  that pays back the bug described in Motivation §1.
- **P4 — documentation.** Semantics table, the conversion matrix, and a "which
  timestamp type should I use" guide.

## Testing

1. `proto-conv` compatibility test, following `v157_type_timestamp_tz.rs`.
2. `TIMESTAMP_NTZ` round-trip SQL logic tests; every expression whose operands
   are all `NTZ` asserted under `UTC`, `Asia/Shanghai`, and
   `America/New_York` in the same run (I1). This is the only reliable way to
   catch a leaked `ctx.func_ctx.tz`.
3. The full conversion matrix: all six directed pairs, each with the session
   timezone varied, asserting the numeric-effect table, plus the three implicit
   mixes (comparison, `UNION`, join key) resolving to the documented target
   type. Includes the `TZ` → `NTZ` property: an offset-bearing value casts to
   `NTZ` keeping its own wall clock under every session timezone.
4. `timestamp_type_mapping` tests: bare `TIMESTAMP` and `DATETIME` under all
   three values, `SHOW CREATE TABLE` printing concrete types, and a prepared
   statement not re-resolving after the setting changes.
5. Negative tests: `%z` / `%Z` on `TIMESTAMP_NTZ`, `TIMESTAMP_NTZ(6)`, a
   non-zero offset in a string cast, TTL on `TIMESTAMP_NTZ`.
6. Planner tests for "Where the session timezone enters": folding under a
   non-UTC timezone, range-index pruning with a mixed-type predicate, and plan
   cache reuse across different `timezone` / `timestamp_type_mapping` values.
7. Parser ambiguity cases: `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP WITH TIME
   ZONE`, `TIMESTAMP(6)`, `DATETIME`.
8. P3 interop: read an `isAdjustedToUTC = false` Parquet file under a non-UTC
   session and assert the wall clock is unchanged.

## Open questions

1. **Coercion precedence.** `TZ > LTZ > NTZ` is proposed. Every pair should be
   spot-checked against Snowflake, since the precedence chooses the target type
   for joins and set operations.
2. **LTZ on-disk encoding.** Should `TIMESTAMP` be written as
   `Timestamp(us, Some("UTC"))` so that it is honest on disk and naturally
   distinct from `TIMESTAMP_NTZ`? It is the cleaner long-term encoding but it
   changes the logical annotation of new files written for existing tables, and
   it changes what external engines see. Suggested as a separate proposal.
3. **Cost expectation.** Adding `TimestampTz` touched 84 files and about 2.2k
   lines and kept producing boundary fixes for months afterwards. `TimestampNtz`
   should be cheaper per site because the physical type is reused and there is
   no offset field, but P0 is what makes that estimate hold. Implicit
   conversions are the part that can leak into planning, so the tests in item 6
   are not optional.
