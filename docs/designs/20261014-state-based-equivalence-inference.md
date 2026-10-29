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

This follows the same principle as normalized range constraints in
[#20653](https://github.com/databendlabs/databend/pull/20653) and
[#20668](https://github.com/databendlabs/databend/pull/20668): maintain the
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
reach 4,950 conditions. This is a synthetic stress case, not a claim about a
measured production class size. Expression construction, cloning, deduplication,
column analysis, and join classification process this expanded representation.
Some redundant conditions are then removed after join reordering.

A concrete workload reference is the repository's
[Q17-shaped histogram/join-order replay](../../src/query/sql/test-support/data/cases/regressions/q17_histogram_join_order.yaml).
It combines `part.p_partkey`, the outer `lineitem.l_partkey`, and a correlated
aggregate's `lineitem.l_partkey`. It is useful for checking connection discovery
and histogram-sensitive join selection, but does not establish the performance
impact of a 100-member class. Such a claim requires measurements on a workload
with the relevant class sizes.

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

#### Asserted Contexts and Region Ownership

Facts may be collected only from top-level conjuncts of a `Filter` or the `ON`
clause of an inner join, where each conjunct is required to evaluate to TRUE.
Flatten only conjunctions in that asserted context. NULL and FALSE both reject a
row there; they are not interchangeable values in an arbitrary expression.

Do not collect facts below `NOT`, `IS [NOT] NULL`, `coalesce`, `OR`, `CASE`/`IF`,
or a projection. For example:

```sql
WHERE (a = 1 AND a = 2) IS NULL
```

For a NULL `a`, both comparisons and their conjunction are NULL, so the outer
predicate is TRUE. Absorbing the comparisons as facts and replacing the
conjunction with FALSE would incorrectly reject that row. Preserve the contract
of `EquivalentConstantsVisitorInner::asserted`: an asserted equality may be
substituted inside nested functions, but merely evaluating a nested equality
does not establish a new fact. Contradiction means the asserted context cannot
be TRUE; it does not authorize replacing a nested expression with FALSE.

Each filter scope and reorderable inner-join region owns its facts. OR branches
must not contribute to one shared state; any future branch-local reasoning must
start a separate context and cannot export branch-specific facts to its parent.
Outer/Semi/Anti joins and subqueries remain boundaries governed by their existing
rules. In particular, do not promote ordinary equality facts from an outer
join's nullable side to its output.

A parent can combine child facts only when its operator preserves them. Alias
handling maps already-established facts to visible output identities; it does
not collect a projected Boolean expression as an assertion. Views are local to
subtrees/regions, not one flat query-global class map. Equivalent rewrites within
a region preserve the facts even if the predicate representation changes.

#### Normalized State

Within that scope, collect equalities and constraints into a shared state. Its
conceptual contents are:

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

The view builder owns the semantic contract: members are safely equivalent for
the equality domain it exposes. Apply the conversion safety described in
[#19827](https://github.com/databendlabs/databend/issues/19827) when admitting facts
or normalizing their types, rather than making each consumer re-prove equality.
Consumers still check whether an expression's dependencies are available. General
function substitution is a separate operation with stricter eligibility below.

Keep a sufficient set of executable proof conditions, not merely an optional
property cache. For supported ordinary equalities, an original spanning forest
can enforce the class without materializing its closure. Preserve required NULL
rejection and retain the existing explicit path for unsupported cases. The query
must remain correct when join reordering is disabled or falls back.

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
under arbitrary overloaded functions. For example, floating-point signed zeros
compare equal but can have different string representations. Identical underlying
types alone therefore do not establish arbitrary substitution safety.

Bound optional propagation by target count, total attempts, and input/output
expression size. Target selection must be stable, and repeated passes must not
silently reset limits in a way that permits uncontrolled derivation growth.
Budget exhaustion must never remove original filters or necessary join conditions.

### 3. Query Connections Before Generating Join Predicates

DPhyp obtains a read-only equivalence view for the current reorder region through
the plan's logical-property path. `InferFilter` exposes its normalized facts there;
DPhyp does not retain the mutable inference optimizer or recover the relationship
from an expanded predicate closure. Plan rewrites must preserve or derive the
view, remapping output identities where necessary. Cache invalidation may rebuild
it from the retained proof conditions, without generating transitive predicates.

DPhyp queries whether two relation sets have an available equality connection,
using class membership and complete expression dependencies. A minimal conceptual
interface is:

```rust
fn neighbors(&self, input: &RelSet) -> impl Iterator<Item = RelationId>;
fn connected(&self, left: &RelSet, right: &RelSet) -> Option<Connections<'_>>;
fn materialize(
    &self,
    connections: Connections<'_>,
    left: &JoinInput,
    right: &JoinInput,
) -> Result<ConditionSet>;
```

`Connections` contains handles to applicable classes or existing explicit edges,
not prebuilt `ScalarExpr` equalities. `JoinInput` supplies available expressions
and equalities already enforced by that child. Discovery must not materialize a
condition for every possible member pair.

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

Keep DPhyp's relation-set enumeration, exclusions, and fallback strategy. Add class
membership to its neighbor and connectivity queries instead of replacing each
class with pairwise `JoinEdge`s or an all-members-required hyperedge. Existing
explicit hyperedges remain responsible for unsupported expressions and their
multi-relation dependencies. An ordinary input condition covered by a class must
not also create a duplicate explicit search edge.

This changes the source of connectivity, not the join-enumeration algorithm. It
does not require fewer candidates; it avoids constructing the full expression
closure merely to make those candidates discoverable.

### 4. Generate a Sufficient Condition Set for Each Join

After a candidate split is selected, generate conditions that preserve the
original constraints, accounting for equalities already established in each child.

For example, if the left child already enforces `a = c` and the right child
already enforces `b = d`, one safe crossing equality such as `a = b` connects all
four members. If those child equalities are not established, one crossing
condition is not necessarily sufficient. In particular, for
`l.a = r.a AND l.b = r.a`, the two left keys cannot be treated as already equal
just because the region's class contains both. The selected join must connect
all groups not yet connected inside its children. For a class with g such groups,
a spanning connection needs g-1 conditions, not every pair of members.

The goal is a sufficient, preferably small condition set for the actual plan,
not a universal representative or a globally minimal set. Expressions depending
on multiple relations are usable only when their full dependencies are available.
Keep original conditions as a fallback when coverage cannot be established.

Join-condition deduplication remains a final safety net, not the source of search
connectivity. Candidate construction should already avoid conditions implied by
its child views. Keep the existing dedup pass for mixed/fallback plans until its
removal is independently justified; do not move it before discovery or rely on it
to repair missing constraints.

### 5. Cardinality and Plan Compatibility

Phase 1 is a behavior-preserving state refactor: keep predicate output and
cardinality estimates unchanged, using existing plan and statistics goldens as
the oracle. It must not change selectivity formulas. Any ordering-only snapshot
change needs an explanation, not a blanket golden refresh.

Phase 2 can intentionally add target filters or decline unsafe/non-deterministic
copies. Estimates and plans may change as a direct result, but unrelated changes
to selectivity combination are outside that phase. Identify each changed plan
and tie it to the intended propagation difference.

Phase 3 can change estimates when redundant equalities stop being materialized.
Use the same child-established groups for condition generation and estimation:
pass the materialized `ConditionSet` to the existing join estimator, rather than
estimating an eager closure and deduplicating later. Charge only connections not
already enforced by a child. Do not add a redundant local filter solely to
reproduce an old estimate, and do not let final join-condition dedup be the first
point at which duplicate charging is noticed.
This is not a proposal for a general correlation estimator; genuinely independent
keys still require the existing selectivity model.

For phase 2/3, review before/after TPC-H and TPC-DS plan and cardinality diffs,
including Q17 and the repeated-key histogram cases in
[`join_cardinality/statistics.rs`](../../src/query/sql/tests/it/optimizer/join_cardinality/statistics.rs).
Benchmark changed representative queries, recording planning time, chosen join
order, intermediate rows, and execution time under the same data and statistics.
A golden update is evidence of an explained change, not evidence of a performance
improvement. Unexplained estimate or join-order changes block rollout.

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

The representation remains optimizer-internal. A subtree/region owns an immutable
view for consumers; inference can update its builder while collecting facts.
Sibling and nested regions do not share a mutable class map. Derivation and cache
invalidation must respect output visibility and operator semantics.

## Rollout Plan

1. **Establish shared normalized state.** Merge classes and their constraints as
   asserted facts arrive; new constraints update the current class root. Stop
   storing/rebuilding the full input adjacency graph for class inference. Retain
   existing equality output, propagation behavior, estimates, and DPhyp inputs.
2. **Move filter inference to target-based queries.** Replace first-neighbor
   substitution with whole-predicate target queries. Add safety checks,
   deterministic deduplication, and bounded optional propagation; preserve distinct
   volatile occurrences. Keep DPhyp's existing connection interface in this phase.
3. **Move join consumers to state-based discovery and generation.** Pass scoped
   views through the plan, integrate class queries with existing explicit edges,
   and materialize sufficient conditions for actual candidate splits. Unsupported
   computed/cross-type/null-safe cases may retain the old path.

Phase 3 is complete for a supported equality domain only when:

- Disabling/failing DPhyp still leaves sufficient executable proof conditions.
- Class membership drives neighbors and connectivity without an eager predicate
  closure or pairwise class-edge expansion; existing enumeration remains intact.
- Small-region tests preserve legal connections and join alternatives relative
  to eager closure, including nonadjacent members and multi-key inputs.
- Constructed joins enforce all original constraints, with dedup and cardinality
  accounting consistent with the child views.
- NULL/context regressions, workload plan diffs, and the benchmark gates above
  pass. Unsupported domains have an explicit tested fallback.

Only phase 3 removes all-pairs materialization for its supported domains. None of
the phases may truncate required equality conditions to meet a propagation budget.

## Testing Strategy

Correctness and plan tests should cover:

- Chain, star, and reordered inputs describing the same equivalence relationship.
- Merged bounds, exclusions, contradictions, and repeated references, including
  permutations that place a bound before or after its class merge.
- The existing [`filter_semantics.test`](../../tests/sqllogictests/suites/query/filter_semantics.test)
  regressions: `(a = 1 AND a = 2) IS NULL` must retain its NULL row; `NOT`, `IF`,
  and `coalesce` must preserve three-valued results. Run this suite at each phase,
  not just after DPhyp changes. Add direct OR-branch and projection-result checks;
  branch facts cannot escape and projected NULL must not become FALSE.
- Multi-column predicates, incompatible types, and nondeterministic expressions.
- Nullable self-equality, null-safe equality, and outer/semi/anti join boundaries.
- Partial rewrites, duplicate derivations, and budget behavior across passes.
- Discovery of joins between members not adjacent in the original input, with no
  pairwise class edges present; compare small-region connectivity against eager
  closure and check both normal and reversed split orientations.
- Constraint coverage across child subplans, multiple keys on one base input,
  aliases, nested independent regions, and multi-relation dependencies.
- Disabled reordering, failed search, and unsupported-domain fallback paths.

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

- Which computed-expression and cross-type domains should extend the initial
  view without weakening its semantic contract?
- How should connection handles fit the existing DPhyp edge identifiers while
  keeping dependencies and child-established groups cheap to query?
- Should propagation budgets be shared across passes, and should targets be
  ranked by expected filtering benefit?
- What provenance is needed to avoid duplicate selectivity application without
  introducing another large intermediate representation?
