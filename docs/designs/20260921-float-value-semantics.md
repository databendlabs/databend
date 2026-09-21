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
by a later query or a later Databend version: bloom filters built from SQL hash
functions, count-min sketch and TopN entries in table metadata, ngram digests,
and any new hash that is serialized.

## Rules

### R1. Value preservation

The bits of a float that Databend stores, transports, or returns are never
rewritten. `-0.0` written by the user is `-0.0` on read; a NaN payload survives
a round trip through storage and the wire. Canonicalization happens only when a
key boundary derives a comparison, order, hash, or key from the value, and it
operates on a copy.

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

### R7. Persisted digests are wire formats

A persisted digest may not change for any input value without, in the same
change:

1. a new format version for the structure that stores it;
2. reader compatibility for the previous version, either by probing every
   representative of the class against the old digest, or by returning
   `Uncertain`/no-prune for values the old digest could misplace;
3. golden-vector tests that pin the exact digest of a fixed input set including
   `+0.0`, `-0.0`, `f64::NAN`, a negative-signed NaN, `-inf`, `+inf`, and the
   corresponding `f32` values.

Query-local digests are exempt from versioning and may change freely, but must
satisfy R4.

### R8. Index soundness

An index or pruning structure may over-approximate (report `Uncertain`, or keep
a block that does not match) but must never under-approximate the predicate's
equality class. Probing with the canonical representative must find every block
that contains any member of the class. A range bound of `-0.0` covers `+0.0`
and vice versa.

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
- Every persisted digest has the golden-vector test required by R7.
- Ordering tests do not assert a specific relative order of `-0.0` and `+0.0`,
  or of distinct NaN payloads.
