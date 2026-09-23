# Float Value Semantics: Equality, Ordering, and Hashing

This document specifies how `FLOAT` (`f32`) and `DOUBLE` (`f64`) values are
compared, ordered, and hashed at every key boundary in Databend. It is
normative. Arithmetic, casts, literal parsing, display, `VARIANT`/JSON, and
Parquet statistics conventions are outside its scope.

## Definitions

**Value.** A concrete `f32`/`f64` bit pattern. Two values are *bit-identical*
if their `to_bits()` agree.

**Equality class.** The set of values that SQL treats as the same value. There
are three kinds of class:

- the *zero class* `{ -0.0, +0.0 }`;
- the *NaN class*, containing every bit pattern with an all-ones exponent and a
  non-zero mantissa, regardless of sign or payload;
- every other value forms a class of size one. `-inf` and `+inf` are ordinary,
  distinct, orderable values.

**Canonical representative.** One fixed value chosen from each class: `+0.0`
for the zero class and `f32::NAN` / `f64::NAN` (the positive quiet NaN the Rust
constants produce) for the NaN class. Singleton classes are their own
representative.

**Key boundary.** Any code path that derives something from a float other than
the float itself: a comparison result, a sort position, a hash, a serialized
key, an index entry, or a statistics bound. This includes `=`, `<>`, `<`, `<=`,
`>`, `>=`, `IN`, `ORDER BY`, `GROUP BY`, `DISTINCT`, join build/probe keys,
`REPLACE INTO` and `MERGE INTO` conflict keys, bloom filter entries, range
index min/max, cluster keys, row encodings, and every `Hash`-like trait
implementation.

**Query-local digest.** A hash whose lifetime ends with the query: group-by
buckets, join hash tables, in-memory `DISTINCT` sets, conflict digests
exchanged between nodes of the same query.

**Persisted digest.** A hash written to storage or table metadata and read back
by a later query or a later Databend version. See the inventory in R7.

## Rules

### R1. Value preservation

Canonicalization happens only when a key boundary derives a comparison, order,
hash, or key from a float, and it operates on a copy. No key boundary rewrites
the value it reads.

What is preserved depends on the encoding of the path the value travels
through:

- **Binary encodings** (column data files, block/segment metadata, binary
  serde, inter-node exchange, binary result formats such as Arrow) preserve the
  exact bit pattern. `-0.0` written by the user is `-0.0` on read; a NaN sign
  and payload survive a round trip.
- **Human-readable encodings** (JSON serde with `is_human_readable()`, textual
  result formats, CSV/TSV, `to_string`) preserve the equality class, the sign
  of zero, and the sign of infinity. They are not required to preserve NaN sign
  or payload: every NaN may be emitted as a single token and decoded as the
  canonical NaN. A human-readable encoding must never turn a NaN into a
  non-NaN value, a zero into a non-zero value, or `-0.0` into `+0.0`.

A path that mixes both (for example a binary value encoded to JSON for a
session variable and decoded again) is a human-readable path for the purpose
of this rule.

### R2. Equality

Two floats are equal if and only if they belong to the same equality class:

- `-0.0 = +0.0` is true;
- `NaN = NaN` is true for any two NaN bit patterns; `NaN <> 1.0` is true;
- `+inf = +inf` is true; `+inf = -inf` is false.

This relation is the equality used by `=`, `<>`, `IN`, `GROUP BY`, `DISTINCT`
(single and multi-argument), `COUNT(DISTINCT ...)`, join key matching,
`REPLACE INTO` and `MERGE INTO` conflict detection, and every index probe. No
key boundary may use bit-identity or IEEE `==` (under which `NaN == NaN` is
false) as its equality.

### R3. Total order

Floats are totally ordered as

```
-inf < negative finite < zero class < positive finite < +inf < NaN class
```

Members of the same class compare `Equal`. Their relative order is unspecified:
no consumer may depend on `-0.0` sorting before or after `+0.0`, or on one NaN
payload sorting before another.

This order is used by the comparison operators, `ORDER BY`, top-N, sort
kernels, row encodings used for sorting or range partitioning, `MIN`/`MAX`,
`ARG_MIN`/`ARG_MAX`, column statistics min/max, range index bounds, and cluster
key ordering.

An order-preserving encoding (for example a byte-comparable row encoding) must
map every member of a class to the same bytes, so that byte comparison agrees
with this order. An encoding derived from IEEE total order (`f64::total_cmp`)
does not satisfy this rule: it separates `-0.0` from `+0.0` and places
negative-signed NaN below `-inf`.

R2 is the `Equal` case of R3: `a = b` implies neither `a < b` nor `a > b`.

### R4. Hash consistency

For every hash computed over a float at a key boundary, same class implies same
hash. This applies to `std::hash::Hash`, `FastHash`, `BloomHash`, `AggHash`,
`DFHash` and the SQL hash functions built on it, fixed-key group-by methods that
reinterpret `f32`/`f64` as `u32`/`u64`, serialized group or join keys,
`approx_count_distinct` inputs, and `REPLACE`/`MERGE` conflict digests.

The hash input is the bit pattern of the canonical representative.
Implementations obtain that pattern through a single shared canonicalization
helper (`OrderedFloat` is the reference implementation); calling `to_bits()` or
`transmute` on the raw value in a key path is non-compliant, because it hashes
bit-identity rather than class.

### R5. Composite keys

Array, Map, Tuple, and Vector values used as keys apply R2, R3, and R4 to their
float elements position by position. A composite hash must also encode enough
structure that distinct composite values do not collapse:

- the container length, or a terminator, so `[1]` and `[1, 1]` differ;
- a NULL marker at each nullable position, so `[NULL, 1]`, `[1, NULL]`, and
  `[0, 1]` differ;
- for Map, the key and value of each entry in the order the type defines.

### R6. `Hash` agrees with `Eq`

Any type used as the key of a `HashSet`/`HashMap`, or of any structure that
requires `Hash + Eq`, must satisfy `a == b ⇒ hash(a) == hash(b)` for every
variant, not only for floats. A type whose `Eq` compares some variants
semantically while its `Hash` covers raw bytes is not a valid key. Code that
needs a semantic key must either make `Hash` and `Eq` agree for all variants or
introduce a dedicated key type whose `Hash` and `Eq` are both defined on the
canonical form.

### R7. Persisted digests

**Inventory.** At the time of writing, the following digests are persisted.
Every one of them hashes a single scalar leaf value; none hashes a composite
(Array, Map, Tuple, Vector) key.

| Structure | Hash path | Float input | Status |
|---|---|---|---|
| Bloom filter (Xor8 / BinaryFuse) in block index files | `DFHash` via `siphash` / `city64`; Map columns hash each value element as a scalar | raw bits | migration governed by this rule |
| Ngram filter in block index files | `DFHash` on `&str` | none | not affected |
| Count-min sketch and TopN in snapshot statistics | `std::hash::Hash` of `ScalarRef` over `supported_stat_type` columns | `OrderedFloat::hash` (canonical) | already compliant |
| HLL NDV (`MetaHLL`) in block and segment statistics | `std::hash::Hash` of `F32` / `F64` | `OrderedFloat::hash` (canonical) | already compliant |

Group-by, join, `DISTINCT`, `REPLACE INTO`/`MERGE INTO` conflict digests, and
the row encoding used for sorting are query-local. Composite-key hashes occur
only on those query-local paths, so R5 changes to composite hashing are not a
persisted-format compatibility problem today. Adding a persisted digest over a
composite key, or extending an existing persisted structure to composite
columns, brings it under this rule. Changing `ScalarRef::hash` for any variant
in `supported_stat_type` changes the count-min sketch digest and is a
wire-format change; changing it for other variants (for example to satisfy R6
for Bitmap or Geometry) is not.

A persisted digest is read from structures written before the current code
existed. Writers follow R4 and hash the canonical representative. Readers must
not assume that a stored digest was produced from the canonical representative,
because structures written before R4 was applied hold digests of whatever bit
pattern the writer received.

**Raw-bit digest.** For every persisted digest, the reader has access to a
digest function that operates on the raw bit pattern of a float without
canonicalization. It is distinct from the R4 key-boundary hash, exists only to
probe persisted structures, and is not used on any other key boundary. Because
the R4 hash of a canonical representative equals the raw-bit digest of that
same bit pattern, the raw-bit digest is sufficient to probe both legacy and
current structures.

**Probe set.** When a reader probes a persisted digest with a float value, it
proceeds per equality class of the probe value:

- zero class: compute the raw-bit digest of both `+0.0` and `-0.0` and treat
  the structure as matching if either probe matches;
- singleton classes: compute the raw-bit digest of the value;
- NaN class: do not probe the digest; return `Uncertain`/no-prune.

The zero class and singleton classes are covered exactly, for structures of
any age. The NaN class cannot be covered by a digest probe, because a legacy
structure may hold a digest of any NaN payload. Equality pruning for NaN is
instead served by range statistics: under R3, NaN is the greatest value, so a
block contains a NaN if and only if its maximum bound is NaN. Column min/max
statistics have always been computed through this order, so the property holds
for statistics written before R4 as well.

**Changing a digest.** Changing the digest for any input value in a way not
covered by the probe set above is a wire-format change and requires a new
format version with reader compatibility for every previous version.

**Tests.** Each persisted digest has golden-vector tests that pin the exact
raw-bit digest of `+0.0`, `-0.0`, `f64::NAN`, a negative-signed NaN, `-inf`,
`+inf`, and the corresponding `f32` values, and a compatibility test that
builds the structure with the pre-R4 writer from a column containing `-0.0`
and a NaN of payload A, then probes it through the current reader, asserting
that:

- probing with `+0.0` and with `-0.0` does not prune the block and goes
  through the raw-bit digest path;
- probing with a NaN of payload B, where B differs from A in sign or mantissa
  bits, returns `Uncertain` from the digest and the block is retained by range
  pruning; probing with payload A itself is not sufficient coverage.

Query-local digests are not persisted and may change freely, but must satisfy
R4.

### R8. Index soundness

An index or pruning structure may over-approximate (report `Uncertain`, or keep
a block that does not match) but must never under-approximate the predicate's
equality class. Probing with the probe set of R7 must find every block that
contains any member of the class. A range bound of `-0.0` covers `+0.0` and
vice versa, and a maximum bound of NaN means the block may contain any NaN.

### R9. Non-finite bounds

Column statistics, domains, and range bounds may contain `-inf`, `+inf`, or
NaN. Consumers must not assume bounds are finite. When a bound is NaN, the
derived domain is the full domain of the type, never an empty or inverted
range.

## Conformance Tests

- Every key boundary that handles floats has a regression test covering at
  least `+0.0`, `-0.0`, `1.0`, `'nan'::DOUBLE`, and one NaN produced by
  arithmetic (for example `sqrt(-1)`, whose bit pattern may differ from the
  `f64::NAN` constant). The test asserts that the zero class forms one
  group/match/key and the NaN class forms one.
- SQL-level coverage in the sqllogictest suites exercises, for the same inputs:
  `=`, `<>`, `ORDER BY`, `GROUP BY`, `DISTINCT`, `COUNT(DISTINCT)`, hash join,
  `IN`, `REPLACE INTO`, `MERGE INTO`, and bloom/range pruning of a Fuse table.
- Every persisted digest has the golden-vector and compatibility tests
  required by R7.
- Ordering tests do not assert a specific relative order of `-0.0` and `+0.0`,
  or of distinct NaN payloads.
