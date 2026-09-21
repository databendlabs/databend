# Select Alias Resolution Matrix

This document defines the theoretical matrix family for Databend's supported
SELECT-alias resolution. The matrix family defines the SQL situations that have
distinct alias-resolution meaning.

## Problem

SELECT aliases are output names, but Databend also supports using them as input
names in several positions of the same query block. That creates a name
resolution problem: the same spelling can refer to an input column, a SELECT
alias, a GROUP BY local alias, or nothing visible in the current scope.

The matrix answers three questions:

- Which SQL situations can resolve a user-written name?
- Which dimensions make two situations semantically different?
- How is one local coordinate encoded as a stable, parseable key?

The matrix is not a model of binder structs. Implementation names must not be
matrix dimensions.

## Supported Surface

The matrix family describes Databend's SELECT/output alias behavior inside a
single SELECT query block and at query-block boundaries.

Included features:

- A SELECT-list item can define an output alias.
- A later SELECT-list item can reference an earlier SELECT alias.
- WHERE, GROUP BY, HAVING, QUALIFY, ORDER BY, aggregate arguments, and window
  specifications can resolve names against the query block's supported alias
  namespaces.
- GROUP BY-family syntax can resolve SELECT aliases and can also create local
  aliases from earlier GROUP BY items for later GROUP BY expressions.
- GROUPING SETS, ROLLUP, CUBE, and combined GROUP BY forms introduce grouping
  item scope boundaries.
- Alias expressions have expression kinds: scalar, aggregate, window, and SRF.
  Those kinds affect whether an alias is legal in a consumer position.
- Query blocks, subqueries, CTEs, and named-window paths define visibility
  boundaries for alias metadata.

Outside this matrix family:

- table aliases and relation aliases;
- CTE names as relation names;
- database, table, and catalog qualification;
- correlated outer-column lookup;
- star expansion;
- generated column names that are not used as SELECT/output aliases.

Those are name-resolution features, but they are not SELECT-alias resolution.
They need separate matrices if they become the target.

## Coordinate Surface

The main coordinate is global:

```text
consumer x name_shape x producer
```

The main coordinate is shared across the matrix family. It identifies the
consumer, the syntactic shape of the name, and the namespace that could produce
that name.

The local coordinate extends the main coordinate with a fixed local context
segment. The segment's shape is global; its interpretation is matrix-local.

## Global Coordinate

### Consumer

The consumer is the clause or syntax position that resolves a name. This is the
first dimension because SQL does not use one global alias rule for every clause.

GROUP BY-family consumers are separate because alias state can be scoped by
item list, grouping set, or expansion result.

### Name Shape

The name shape is the syntactic form being resolved. It determines whether the
consumer can perform whole-item alias lookup or must bind names inside an
expression.

This dimension must stay separate from producer. For example, `GROUP BY k` and
`GROUP BY k + 1` may mention the same spelling, but they are different rules
because only the bare form can be a whole-item alias reference.

### Producer

The producer is the namespace that could provide the referenced name.

Producer describes the SQL namespace, not the code path. If the implementation
changes from one helper type to another, this dimension stays the same.
Do not encode conflicts as composite producers. For example, a prior GROUP BY
alias that conflicts with an input column is still `prior-group-alias`; the
input-column conflict belongs outside the producer coordinate.

### Code Set

For the first three segments, the letter names a semantic subfamily inside the
dimension and the digit names a member of that subfamily. For example, `G0`,
`G1`, and `G2` are GROUP BY-family consumers, while `P0`, `P1`, and `P2` are
predicate consumers. The digit only needs to be unique inside the same
subfamily, not inside the whole dimension.

Consumer:

- `S0`: `select-list`
- `G0`: `group-by`
- `G1`: `grouping-sets`
- `G2`: `rollup`
- `G3`: `cube`
- `G4`: `combined-group-by`
- `G5`: `group-by-all`
- `P0`: `where`
- `P1`: `having`
- `P2`: `qualify`
- `W0`: `window`
- `O0`: `order-by`
- `A0`: `aggregate-argument`
- `B0`: `query-block-boundary`

Name shape:

- `N0`: `bare`
- `N1`: `qualified`
- `N2`: `complex`
- `N3`: `qualified-complex`
- `N4`: `ordinal`
- `N5`: `implicit-item-collection`

Producer:

- `I0`: `input-column`
- `S0`: `prior-select-alias`
- `S1`: `select-scalar-alias`
- `S2`: `select-aggregate-alias`
- `S3`: `select-window-alias`
- `S4`: `select-srf-alias`
- `G0`: `prior-group-alias`
- `N0`: `none`

## Coordinate Encoding

The key encodes a local coordinate, but the key grammar is global.

The key shape is:

```text
<consumer><name_shape><producer>_<region_index>_<local_context>
```

The first three coordinate codes are the main coordinate and are concatenated
directly. Each coordinate code is exactly two ASCII characters: one uppercase
letter followed by one digit. The region index is the zero-based `regions`
array position in the owning YAML file.

The encoded form is:

```text
CCNNPP_R_<local_index_sequence>
```

`CCNNPP` is the main coordinate. `R` is the region index. The suffix after the
second `_` is the encoded local context. The second separator is always present.
If no local dimension is selected, the suffix is empty.

Each matrix declares an ordered `local_dimensions` list; the harness assigns bit
positions to those dimensions in that order. When a region selects a local
dimension, the selected value is encoded as that value's zero-based index in
uppercase hex. The local context is the concatenation of the selected dimensions'
indexes, in global local-dimension order. For example, with dimensions
`[[a, b], [c, d]]`, selecting `a` and `d` encodes as `01`.

## Local Coordinate

The local coordinate has a global shape:

```text
main_coordinate x local_context
```

The shape and width are shared across all matrices. The interpretation is not
shared. The owning matrix defines the ordered local dimensions and gives meaning
to the local context. One local coordinate is one semantic situation. It is not
one query and it is not an assertion result.

Local context is not a global semantic dimension. It is interpreted only after
the main coordinate has selected an owning matrix.

The same local context bit can therefore mean different things in different
matrices. That is intentional: relation conflict, grouping-set scope, expression
legality, ordering ambiguity, and query-block boundary are not one shared axis.
Forcing them into a global context would make the main coordinate less stable
and would reserve bits for facts that most matrices cannot contain.

The encoded value is only meaningful inside the matrix selected by the main
coordinate. The same hex value can mean different local facts in different
matrices.

Inside one matrix, dimensions are ordered semantic axes. A region selects zero
or more of those axes through its `local` selector. The harness takes the
Cartesian product of selected values, encodes each selected value in its
dimension's position, and concatenates those indexes into the local context.

## Matrix Family

The alias-resolution model is a matrix family. One main coordinate routes to one
named matrix, and only that matrix defines the local context meaning.

The family is defined by semantic ownership, not implementation modules.

The owning matrix is derived from the main coordinate.
For example, a GROUP BY-family consumer with `select-scalar-alias` or
`prior-group-alias` routes to `group_by_alias`, while the same consumer with an
aggregate, window, or SRF SELECT alias routes to `group_by_alias_kind`.
Coordinates that cannot be routed by `consumer + name_shape + producer` indicate
that the matrix family needs another named matrix or a sharper
producer/name-shape boundary.

### `select_list_alias`

This matrix owns alias references inside the SELECT list itself.

Its defining property is ordered local production. A SELECT item defines an
output alias; a later SELECT item can reference aliases produced earlier in the
same SELECT list. That differs from clauses such as HAVING or ORDER BY, where
the SELECT alias namespace is consumed as a query-block-level namespace.

This matrix owns conflicts between input columns and earlier SELECT aliases,
and it owns the difference between bare-name reuse and reuse inside a complex
expression.

Local context axes:

```text
relation x select_list_scope x setting
```

### `group_by_alias`

This matrix owns GROUP BY-family consumers:

- `group-by`
- `grouping-sets`
- `rollup`
- `cube`
- `combined-group-by`

It owns name resolution between input columns, SELECT scalar aliases, and local
GROUP BY aliases. Its defining property is that GROUP BY has two alias
mechanisms that interact:

- SELECT-list aliases can be used by GROUP BY.
- Earlier GROUP BY items can create names for later GROUP BY expressions.

This matrix also owns grouping construct scope. `GROUPING SETS`, `ROLLUP`, and
`CUBE` are not only syntactic variants of GROUP BY; they create expanded item
lists and set boundaries. That makes same-list reuse, same-set reuse,
separate-set isolation, and generated-single-expression sets part of the same
semantic region.

Local context axes:

```text
relation x group_scope x expansion x setting
```

Local context bit layout:

```text
0001  relation.input-conflict
0002  relation.no-input
0004  relation.alias-name-conflict
0010  scope.same-list-exact
0020  scope.same-list-case-insensitive
0040  scope.same-list
0080  scope.same-set
0100  scope.separate-set
0200  scope.no-prior-group-alias
0400  expansion.generated-single-expression-set
0800  expansion.normal-plus-set
1000  setting.alias-first
2000  setting.column-first
```

The relation and setting groups are exclusive. Scope and expansion bits are
chosen according to the grouping construct being modeled.

### `group_by_alias_kind`

This matrix owns GROUP BY-family references to SELECT aliases whose expressions
are not plain scalar expressions:

- aggregate SELECT aliases
- window SELECT aliases
- SRF SELECT aliases

It is separate from `group_by_alias` because the distinguishing question is
whether the producer is a legal grouping expression producer at all, not alias
precedence or grouping-set scope.

Local context axes:

```text
relation x ambiguity x function_kind
```

### `predicate_alias`

This matrix owns predicate consumers:

- `where`
- `having`
- `qualify`

Its defining property is predicate binding against the query block's visible
alias namespaces. These consumers all resolve boolean predicates, but they do
not allow the same producer kinds. Scalar, aggregate, window, and SRF aliases
have different legality in WHERE, HAVING, and QUALIFY. Keeping them together is
sound because the consumer dimension still exposes clause-specific legality.

Local context axes:

```text
relation x legality x setting
```

### `order_by_alias`

This matrix owns ORDER BY alias lookup.

ORDER BY is separate from predicate consumers because it is an ordering
expression, not a boolean predicate, and it has special alias behavior:
duplicate SELECT aliases can make an ORDER BY name ambiguous, and ORDER BY can
be a source of aliases later reused inside window specifications.

Local context axes:

```text
relation x ambiguity x setting
```

### `group_by_name_bypass`

This matrix owns GROUP BY-family syntax that does not resolve a user-written
alias name:

- ordinal references, such as `GROUP BY 1`
- implicit item collection, such as `GROUP BY ALL`

It is separate from `group_by_alias` because these forms are boundaries of the
alias-resolution surface, not ordinary alias lookup cases. Keeping them separate
prevents non-name facts from consuming bits in the normal GROUP BY local context
layout.

Local context axes:

```text
non_name x setting
```

Local context bit layout:

```text
0001  non-name.select-item-position
0002  non-name.non-aggregate-select-items
1000  setting.alias-first
2000  setting.column-first
```

The setting group is exclusive. The non-name group is exclusive because one
syntax item is either an ordinal reference or an implicit item collection.

### `aggregate_argument_alias`

This matrix owns alias lookup inside aggregate function arguments.

It is separate because the consumer is inside SELECT expression binding, but the
name being resolved may conflict with an earlier SELECT alias. The important
boundary is that aggregate arguments have their own legality and grouping rules;
they are not just another SELECT-list expression and not a predicate consumer.

Local context axes:

```text
relation x legality x setting
```

### `window_spec_alias`

This matrix owns alias lookup inside window specifications, including PARTITION
BY and ORDER BY inside `OVER (...)`.

It is separate because a window specification can reuse some aliases produced
by surrounding clauses, but must not recursively expand a window alias into
another window specification. It also interacts with grouped queries, where a
group item alias can be the visible name inside the window spec.

Local context axes:

```text
relation x legality x window_boundary x setting
```

### `query_block_boundary_alias`

This matrix owns alias visibility across query-block boundaries:

- subquery expressions
- CTE query blocks
- named window paths

It is separate because the main question is not precedence inside one consumer.
The question is whether alias metadata belongs to the right query block and
does not leak into an inner or outer scope.

Local context axes:

```text
relation x query_block_boundary x setting
```

## Expressive Power

For Databend's supported SELECT/output alias features, every supported alias
lookup can be classified by the main coordinate plus local context:

- where the name is consumed;
- which syntactic shape is being resolved;
- which alias namespace can produce it;
- which matrix-local facts change that lookup.

The first three facts are the global dimensions. The last fact is interpreted by
the owning matrix. The matrices are split at these semantic discontinuities:

- ordered alias production in the SELECT list
- GROUP BY's mix of SELECT aliases and local group-item aliases
- expression-kind legality for scalar, aggregate, window, and SRF aliases
- predicate consumers with clause-specific legality
- ORDER BY ambiguity and ordering-name reuse
- GROUP BY non-name syntax
- aggregate-argument name binding
- window specification binding and recursive window-alias boundaries
- query-block boundaries

The family is not strong enough for all SQL name resolution, and it does not
try to be. Table aliases, CTE relation names, database/table qualification,
correlated outer-column lookup, and star expansion use different producer
namespaces and different visibility rules. Mixing them into this family would
weaken the matrix by making `producer` mean too many unrelated things.

## Key Examples

```text
G0N0S1_1001
```

GROUP BY resolves a bare name that can be produced by a SELECT scalar alias.
The alias conflicts with an input column, and alias-first mode is active.

```text
G1N2G0_2102
```

GROUPING SETS resolves a complex expression that would need a prior group alias.
There is no input column for the name, the prior alias came from a separate set,
and column-first mode is active.

```text
G0N4N0_0001
```

GROUP BY uses an ordinal. This is a SELECT-item position rule, not alias name
resolution.
