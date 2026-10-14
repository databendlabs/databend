# RFC: State-Based Equivalence Inference and Predicate Generation

## Summary

Databend derives transitive equalities by expanding equivalence classes into
pairwise predicates. A class with m members can produce m(m−1)/2 conditions,
which subsequent optimizer passes must inspect, classify, and sometimes remove.
General predicate propagation, however, uses only one direct equality neighbor
and does not consistently benefit from the same equivalence knowledge.

This RFC proposes treating a normalized inference state, rather than an expanded
predicate list, as the primary representation of equivalence knowledge. The
optimizer collects facts into that state, lets consumers query it, and generates
predicates for a particular scan or join only when they are needed.

This follows the same principle as normalized range constraints: maintain the
strongest bounds and relevant exclusions, rather than repeatedly generating and
merging intermediate predicates. The proposal extends that principle to
relationships between expressions and their consumers in join planning.

## Motivation

### Eager Equality Expansion

Consider expressions of compatible types:

```sql
WHERE a = b AND b = c AND c = d
```

The relationship can be represented as one equivalence class:

```text
{a, b, c, d}
```

Expanding it produces six pairwise equalities. With 100 members, the output can
reach 4,950 conditions. Expression construction, cloning, deduplication, column
analysis, and join classification all process this expanded representation.
Some redundant conditions are then removed after join reordering.

The equalities are useful knowledge. The unnecessary cost comes from expressing
all of that knowledge as standalone predicates before their purpose is known.

### Incomplete Use of Equivalence Knowledge

For example:

```sql
WHERE t1.s = t2.s
  AND t2.s = t3.s
  AND t1.s LIKE '%foo%'
```

General predicate propagation selects one direct replacement neighbor. One
inference invocation may derive the filter for t2 but miss t3, even though the
full equivalence class establishes the opportunity.

These are two consequences of the same representation problem: some consumers
receive an eagerly expanded closure, while others see only local adjacency.

### The Constraint-Normalization Analogy

For numeric constraints:

```sql
WHERE x > 1 AND x > 5 AND x >= 3 AND x < 10
```

A normalized state stores the tightest lower and upper bounds and emits:

```sql
WHERE x > 5 AND x < 10
```

It does not need to retain every intermediate combination. Equivalence inference
should similarly maintain the relationship itself and derive an appropriate
condition set for each consumer. Unlike range bounds, however, there is no single
small predicate list suitable for every join order.

## Goals

- Make normalized state the shared source of equivalence knowledge.
- Generate conditions for a known target instead of eagerly materializing the
  transitive closure.
- Improve filter propagation without enumerating substitution combinations.
- Preserve original constraints, NULL semantics, and legal join alternatives.
- Reduce predicate-processing overhead and enable earlier filtering.

## Non-Goals

- Change SQL comparison, implicit conversion, or function semantics.
- Broaden inference across Outer/Semi/Anti joins or null-safe equality boundaries.
- Find a globally minimal predicate set or explore every filter substitution.
- Guarantee linear join enumeration cost or execution improvements for every query.

## Existing Behavior

`InferFilter` already constructs equivalence classes for constraint inference.
The issue is not the absence of Union-Find, but the lifetime and use of its
result: class information is expanded into equality predicates, while general
predicate propagation consults the original adjacency representation.

DPhyp uses explicit join conditions to discover connections and construct
candidate joins. Consequently, removing inferred equalities without changing
that consumer can remove join alternatives. For `a = b AND b = c`, the inferred
`a = c` allows a and c to join first.

Join-condition deduplication later removes some redundant equalities. This RFC
moves the decision about which expressions to materialize closer to their use;
it does not simply move that deduplication pass earlier.

## Proposed Design

### 1. Maintain a Normalized Inference State

For each semantic region in which inference is valid, collect equalities and
constraints into a shared state. Its conceptual contents are:

```text
InferenceState
  expression -> equivalence class
  class -> members and relation dependencies
  class -> safely merged value constraints
  propagation inputs -> source predicates and substitution restrictions
  original conditions -> semantic scope and fallback obligations
```

These are logical responsibilities, not a prescribed public API. A Union-Find
structure can manage membership; bounds and exclusions can reuse normalized
constraint handling.

For compatible expressions:

```sql
WHERE a = b AND b = c AND a > 1 AND b > 5 AND c < 10
```

The state can represent:

```text
members: {a, b, c}
value constraints: lower = 5 (exclusive), upper = 10 (exclusive)
```

There is no need to materialize every intermediate bound or equality to retain
this information. Conflicting constraints can mark the region unsatisfiable
where permitted by its semantics.

Here, "final state" means the normalized result of the collected facts in that
region, not a permanently fixed state for the whole query. New facts can update
it. Stable membership and traversal should avoid dependence on input ordering
or hash iteration order where equivalent choices exist.

Retain original conditions for validation and fallback. Class membership alone
must not authorize every typed comparison or substitution. Reuse the conversion
safety described in [#19827](https://github.com/databendlabs/databend/issues/19827)
and validate the particular operation being generated.

### 2. Query the State for Scan-Side Filtering

A filter consumer supplies a target column or relation scope. The inference
state provides applicable value constraints and attempts safe substitutions
into that scope.

For the three-table `LIKE` example, each scan can obtain its own filter directly
from the class, without waiting for multiple adjacency-based propagation passes.

For multi-reference predicates, rewrite the entire predicate consistently:

```sql
-- Given t1.a = t2.a and t1.b = t2.b:
some_predicate(t1.a, t1.b)
-- Candidate filter for t2:
some_predicate(t2.a, t2.b)
```

Do not enumerate every mixture of source and target references. Reject a
candidate that cannot be rewritten completely into its target scope. Preserve
original filters and deduplicate derived results.

Only deterministic predicates and replacement expressions are eligible.
Initially, substitution within complex predicates should require identical
underlying types after removing Nullable wrappers, followed by type and scope
validation. Safe equality conversion alone does not establish interchangeability
under arbitrary overloaded functions.

Bound optional propagation by target count, total attempts, and input/output
expression size. Target selection must be stable, and repeated passes must not
silently reset limits in a way that permits uncontrolled derivation growth.
Budget exhaustion must never remove original filters or necessary join conditions.

### 3. Query Connections Before Generating Join Predicates

DPhyp should query whether two relation sets have an available equality
connection, using class membership and complete expression dependencies.
It should not require a standalone `ScalarExpr` for every possible connection.

For `{a, b, c}`, a and c must remain connectable before b participates. Replacing
the class with one fixed star can hide that opportunity from the current graph.
Likewise, a connection that activates only when all class members are available
would not represent the required pairwise opportunities.

Discovery and materialization therefore become separate operations:

```text
normalized state
  -> discover a legal connection for a candidate split
  -> select conditions needed for that split
  -> materialize executable equality expressions
```

This does not require fewer join candidates. It avoids constructing the full
expression closure merely to make those candidates discoverable.

### 4. Generate a Sufficient Condition Set for Each Join

After a candidate split is selected, generate conditions that preserve the
original constraints, accounting for equalities already established in each child.

For example, if the left child already enforces `a = c` and the right child
already enforces `b = d`, one safe crossing equality such as `a = b` connects all
four members. If those child equalities are not established, one crossing
condition is not necessarily sufficient.

The goal is a sufficient, preferably small condition set for the actual plan,
not a universal representative or a globally minimal set. Expressions depending
on multiple relations are usable only when their full dependencies are available.
Keep original conditions as a fallback when coverage cannot be established.

Track the relationship between original and derived conditions sufficiently to
avoid applying selectivity repeatedly to correlated facts. Expression reduction
must not silently change cardinality estimates through inconsistent accounting.

## Correctness and Compatibility

- Nullable `a = a` is not a tautology. Preserve its NULL-rejecting effect.
- Ordinary `=` and null-safe equality require separate semantic handling.
- Equivalence facts remain local to valid inference regions; they must not
  bypass an outer join's preserved-side boundary.
- Equality conversion safety is distinct from arbitrary substitution safety.
- Every original constraint must remain enforced by the resulting plan, even
  when its original expression is no longer explicitly present.

This is an optimizer representation change, not a SQL or storage-format change.
Plan shapes can change as filtering improves. Join discovery should preserve
legal connection opportunities, though heuristic exploration need not produce
identical plans.

## Integration Points

- **InferFilter:** collect and normalize facts; expose membership, constraints,
  and target-specific inference rather than only expanded predicates.
- **Filter pushdown:** request predicates within the allowed scan or join-input
  scope, retaining existing semantic boundaries.
- **DPhyp:** query connections and materialize conditions for candidate splits.
- **Cardinality estimation and join-condition deduplication:** account for
  correlated derivations and keep a consistent view of established equalities.

The shared representation can remain optimizer-internal. The lifetime and
ownership of state across these consumers require explicit design; unrelated
inference regions must not accidentally share facts.

## Rollout Plan

1. **Establish shared normalized state.** Unify equivalence membership and
   constraint access while retaining existing equality output and DPhyp inputs.
   This separates representation changes from join-search changes.
2. **Move filter inference to target-based queries.** Add whole-predicate
   rewriting, safety checks, deduplication, and bounded propagation.
3. **Move join consumers to state-based discovery and generation.** Adapt DPhyp,
   validate constraint coverage and estimation, then remove eager transitive
   equality expansion. Retain fallback behavior for unsupported cases.

Only the third phase removes the all-pairs materialization cost. None of the
phases should use arbitrary truncation of required equality conditions.

## Testing Strategy

Correctness and plan tests should cover:

- Chain, star, and reordered inputs describing the same equivalence relationship.
- Merged bounds, exclusions, contradictions, and repeated references.
- Multi-column predicates, incompatible types, and nondeterministic expressions.
- Nullable self-equality, null-safe equality, and outer/semi/anti join boundaries.
- Partial rewrites, duplicate derivations, and budget behavior across passes.
- Discovery of joins between members not adjacent in the original input.
- Constraint coverage across child subplans and multi-relation dependencies.

Compare optimization time, peak memory, materialized predicate counts, and DPhyp
candidate counts for different class sizes and sparse/dense input graphs.
Retaining members and input edges should avoid additional closure-sized storage,
but already dense inputs and join enumeration may remain expensive.

Execution tests should measure rows entering joins, filter evaluation cost,
cardinality estimates, and total runtime. Pushable filters are not necessarily
indexable or useful enough to offset their evaluation cost.

## Alternatives

- **Expand everything, then deduplicate:** keeps the current consumer interface,
  but pays the construction and intermediate processing cost first.
- **Replace all equalities with a fixed tree:** compact logically, but can hide
  join alternatives unless consumers understand the equivalence state.
- **Propagate to every substitution combination:** improves coverage at the cost
  of potentially combinatorial expression growth.

## Open Questions

- How should state ownership and lifetime align with optimizer and join regions?
- What connection-query interface best fits DPhyp and multi-relation expressions?
- How should candidate generation represent already established equalities and
  prove that its emitted conditions preserve all required constraints?
- Should propagation budgets be shared across passes, and should targets be
  ranked by expected filtering benefit?
- What provenance is needed to avoid duplicate selectivity application without
  introducing another large intermediate representation?
