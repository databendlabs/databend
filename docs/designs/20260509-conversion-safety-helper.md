# RFC: Conversion Safety Helper for Optimizer Reasoning

## Summary

Databend has several mechanisms for type coercion today, including function
type checking, auto-cast rules, common super type inference, and TRY_CAST rules.
These mechanisms answer whether an expression can be type-checked and evaluated,
but they do not describe what kind of conversion is being used.

Some optimizer rules need a stricter question:

> Can this conversion preserve equality semantics well enough to be used for
> equivalence-class inference?

This RFC proposes a reusable conversion-safety helper in
`databend_common_expression` that classifies conversions by their semantic
properties, such as losslessness, injectivity, value-dependence, and TRY_CAST
behavior. `InferFilter` can then consume this helper instead of maintaining
rule-local type compatibility logic.

## Motivation

Issue #17933 exposes a mismatch between expression evaluation and optimizer
reasoning.

For example, Databend can evaluate mixed string/numeric equality:

```sql
SELECT *
FROM (
    SELECT '01' AS s1, '1' AS s2, 1 AS n
) t
WHERE s1 = n AND s2 = n;
```

The predicates `s1 = n` and `s2 = n` can both evaluate to true under numeric
comparison, but they do not imply `s1 = s2`, because `'01' = '1'` is false under
string comparison. Therefore this equality is valid for evaluation but unsafe
as an equivalence-class edge.

Float conversion has a similar problem. Integer or Decimal values can collapse
after conversion to Float, so using such equality as a transitive inference
edge can derive predicates not implied by the original SQL.

The current Databend APIs do not make this distinction explicit.

## Existing Capabilities

Databend already has useful building blocks:

- `check_function` in `src/query/expression/src/type_check.rs` checks whether a
  function call can be typed and inserts casts.
- `FunctionRegistry::{get_auto_cast_rules,is_auto_try_cast_rule}` in
  `src/query/expression/src/function.rs` exposes auto-cast rules and
  TRY_CAST-based rules.
- `can_auto_cast_to` and `common_super_type` in
  `src/query/expression/src/type_check.rs` provide type-level coercion and
  common type inference.
- `NumberDataType::can_lossless_cast_to` and `get_decimal_properties` in
  `src/query/expression/src/types/number.rs` provide partial numeric widening
  information.
- Comparison function registration in
  `src/query/functions/src/scalars/comparison.rs` describes which same-type
  comparison functions exist today.

However, these APIs do not answer whether a conversion is:

- lossless or lossy;
- injective or many-to-one;
- value-dependent, such as String to Number;
- TRY_CAST-based, such as Variant to concrete types;
- safe for equality inference.

Snowflake's data type conversion documentation separates related concepts such
as explicit conversion, implicit coercion, supported conversion pairs, possible
conversion failure, and precision loss:

https://docs.snowflake.com/en/sql-reference/data-type-conversion

Databend would benefit from a similar internal classification layer.

## Goals

- Provide a reusable helper that describes conversion properties independent of
  any single optimizer rule.
- Let `InferFilter` ask for objective conversion facts, then apply its own
  policy.
- Avoid treating `check_function(eq)` as proof that an equality is safe for
  transitive inference.
- Preserve existing useful behavior such as integer/integer inference through a
  Decimal common type, including signed/unsigned integer joins.
- Make unsafe cases explicit: String/Number, Variant/concrete TRY_CAST, and
  Integer/Decimal to Float.

## Non-Goals

- This RFC does not propose changing Databend's SQL evaluation semantics.
- This RFC does not propose removing existing auto-cast or TRY_CAST rules.
- This RFC does not require a complete Snowflake-compatible conversion matrix in
  the first implementation.
- This RFC does not require every type that supports equality to be immediately
  used by `InferFilter`.

## Potential Consumers

`InferFilter` is the primary consumer for the first implementation. Other
optimizer and execution components may need the same distinction between "can be
evaluated" and "safe to reason about" later:

- Join equality reasoning: join reordering, equivalence class construction,
  runtime filter derivation, and join key inference need to know whether an
  equality can safely act as an equivalence edge.
- Predicate pushdown and pruning: storage pushdown, segment pruning, and bloom
  pruning often cast constants or columns before comparing them. They need to
  avoid lossy or value-dependent casts that could prune incorrectly.
- `EquivalentConstantsVisitor`: replacing columns with constants from
  `col = constant` is only safe when the equality preserves the intended value
  semantics.
- Range and constant inference: deriving `a > 10` from `a = b AND b > 10`
  requires conversions that are not only equality-safe but may also need
  ordering properties.
- Bloom filters and runtime filters: applying a filter across differently typed
  keys should avoid conversions that collapse many source values into one target
  value.
- Statistics and histogram reasoning: selectivity estimation over mixed-type
  predicates benefits from knowing whether a conversion is lossy,
  value-dependent, or TRY_CAST-based.

These are future candidates. The first step should keep the implementation small
and validate the API through `InferFilter`.

## Proposed API

Place the helper in `databend_common_expression`, for example under a new
module:

```rust
pub enum ConversionClass {
    /// Same logical type, no conversion needed.
    Identity,

    /// Conversion preserves distinct source values in the target type.
    LosslessInjective,

    /// Conversion is deterministic but can lose information or merge values.
    Lossy,

    /// Whether conversion succeeds or how it compares depends on runtime value
    /// contents, e.g. String -> Number.
    ValueDependent,

    /// Conversion is represented by TRY_CAST or can turn failed conversion into
    /// NULL.
    TryOnly,

    /// No supported conversion is known.
    Unsupported,
}

impl ConversionClass {
    pub fn is_safe_for_equality_inference(&self) -> bool {
        matches!(self, Self::Identity | Self::LosslessInjective)
    }
}

pub struct CommonTypeConversion {
    pub common_type: DataType,
    pub left: ConversionClass,
    pub right: ConversionClass,
}

pub fn classify_conversion(src: &DataType, dest: &DataType) -> ConversionClass;

pub fn common_super_type_with_conversion(
    left: &DataType,
    right: &DataType,
) -> Option<CommonTypeConversion>;
```

The helper describes facts. Callers decide policy.

For example, `InferFilter` would require:

```rust
let conv = common_super_type_with_conversion(left_ty, right_ty)?;
conv.left.is_safe_for_equality_inference()
    && conv.right.is_safe_for_equality_inference()
```

Other callers could accept a broader set of conversions.

This RFC does not introduce a `CastContext` parameter. `CastContext` is useful
when a caller wants to express how permissive a cast operation should be. The
helper proposed here should instead describe objective conversion properties;
callers then map those properties to their own policy.

## First Implementation Rules

The first implementation can be conservative and focused on correctness.

### Identity

Same logical type returns `Identity`, subject to the type being representable in
the expression system. This does not imply every optimizer rule must use all
identity conversions.

Examples:

- `String -> String`
- `Boolean -> Boolean`
- `Timestamp -> Timestamp`
- `Variant -> Variant`

### LosslessInjective

Examples:

- Integer widening when `NumberDataType::can_lossless_cast_to` says it is safe.
- Integer to Decimal when the Decimal can represent all source values.
- Decimal widening when scale and leading digits are preserved.
- `Date -> Timestamp`, because distinct dates map to distinct timestamps at
  midnight.
- `Float32 -> Float64`.
- Nullable wrappers when the inner conversion is safe.
- Array/Tuple recursively when element/field conversions are safe.
- `Null -> T`, because it does not introduce value collisions.
- `EmptyArray -> Array<T>`.

Integer/integer common type should use a Decimal common type when needed to
preserve existing signed/unsigned behavior such as `Int64 = UInt64`.

### Lossy

Examples:

- Integer or Decimal to Float.
- Float to Integer.
- Decimal scale reduction.
- Decimal to a narrower Decimal.

These conversions can be valid for evaluation, but not for equality inference.

### ValueDependent

Examples:

- String to Number.
- String to Boolean.
- String to Date/Timestamp.

These conversions depend on runtime string contents and can have multiple string
representations for the same target value, such as `'01'` and `'1'`.

### TryOnly

Examples:

- Variant to concrete types through registered `auto_try_cast_rules`.

TRY_CAST semantics can convert failed conversions to NULL, so they are unsafe as
equivalence-class edges.

### Unsupported

If no rule applies, return `Unsupported`.

## Common Type Inference

`common_super_type_with_conversion` should reuse existing logic where possible,
but report conversion classes alongside the result.

Suggested behavior:

1. Handle nullable/null/empty-array recursively, reusing the shape of
   `common_super_type`.
2. Handle integer/integer by choosing a Decimal common type that can represent
   both sides.
3. Handle integer/Decimal and Decimal/Decimal by Decimal merge.
4. Handle float/float by choosing the wider Float.
5. Handle Date/Timestamp as Timestamp.
6. Handle Array/Tuple recursively.
7. Mark String mixed conversions as `ValueDependent`, not `LosslessInjective`.
8. Mark Variant auto-try-cast conversions as `TryOnly`.
9. Return `Unsupported` when no conversion path exists.

This should avoid using comparison auto-cast rules as evidence of inference
safety. Those rules are for expression evaluation.

## How InferFilter Uses It

`InferFilter` currently needs to decide whether an equality predicate can add an
edge to its equivalence graph.

With the helper:

```rust
fn check_equal_expr_type(left_ty: &DataType, right_ty: &DataType) -> bool {
    let Some(conv) = common_super_type_with_conversion(left_ty, right_ty) else {
        return false;
    };

    conv.left.is_safe_for_equality_inference()
        && conv.right.is_safe_for_equality_inference()
}
```

This keeps the rule-local policy small and makes the conversion reasoning
shareable.

## Examples

### String and Number

```sql
'01' = 1
'1' = 1
```

Classification:

```text
String -> Number: ValueDependent
Number -> Number: Identity
```

`InferFilter` rejects this equality edge.

### Integer and Decimal

```text
Int64 -> Decimal(19, 0): LosslessInjective
Decimal(18, 2) -> Decimal(21, 2): LosslessInjective
```

`InferFilter` can use this edge.

### Integer and Float

```text
Int64 -> Float64: Lossy
Float64 -> Float64: Identity
```

`InferFilter` rejects this equality edge.

### Date and Timestamp

```text
Date -> Timestamp: LosslessInjective
Timestamp -> Timestamp: Identity
```

`InferFilter` can use this edge.

### Variant and Number

```text
Variant -> Nullable(Number): TryOnly
```

`InferFilter` rejects this equality edge.

## Rollout Plan

1. Add the conversion classification helper in `databend_common_expression`.
2. Add unit tests for scalar conversions and nested Array/Tuple conversions.
3. Migrate `InferFilter` to consume the helper.
4. Keep current SQL behavior unchanged.
5. Add follow-up adopters only when they need the same semantics, such as join
   equality reasoning or pruning-safe predicate derivation.

## Test Plan

- Unit tests for `classify_conversion`:
  - numeric widening;
  - signed/unsigned integer to Decimal;
  - Decimal widening and narrowing;
  - Date to Timestamp;
  - String to Number/Date/Boolean;
  - Variant to concrete;
  - Float precision cases;
  - nullable/array/tuple recursion.
- Unit tests for `common_super_type_with_conversion`.
- Existing `InferFilter` tests for:
  - String/Number transitivity rejection;
  - Number/Decimal propagation;
  - Number/Float rejection;
  - signed/unsigned integer preservation;
  - Date/Timestamp propagation;
  - nested Array/Tuple behavior.

## Open Questions

- Should identity conversion for all same logical types be classified as
  `Identity`, even if no comparison function exists today?
- Should Map be supported recursively, or left unsupported until there is a
  clear equality semantics requirement?
- Should `common_super_type_with_conversion` live next to `common_super_type`, or
  in a new `conversion` module?
- Should conversion classes include both `implicit` and `explicit` flags,
  similar to Snowflake's castable/coercible distinction?
- Should this helper eventually replace parts of `can_auto_cast_to`, or remain a
  parallel semantic classifier?
