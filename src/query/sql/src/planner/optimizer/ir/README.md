# Optimizer IR (Intermediate Representation)

This directory contains the core Intermediate Representation (IR) components of the Databend query optimizer. The IR serves as the foundation for query plan representation, transformation, and optimization.

## Directory Structure

```
ir/
├── expr/                      # Expression system
│   ├── s_expr.rs              # Single expression representation
│   ├── m_expr.rs              # Multi-expression representation
│   ├── extract.rs             # Pattern extraction utilities
│   └── mod.rs                 # Module exports
│
├── implementation.rs          # Storage-independent physical candidates
├── memo.rs                    # Memo structure for storing equivalent expressions
├── group.rs                   # Group representation for equivalent expressions
├── property/                  # Property system
│   ├── builder.rs             # Property builder utilities
│   ├── enforcer.rs            # Property enforcers
│   ├── property.rs            # Core property definitions
│   └── mod.rs                 # Module exports
│
├── stats/                     # Statistics system
│   ├── column_stat.rs         # Column statistics
│   ├── histogram.rs           # Histogram for statistics
│   ├── selectivity.rs         # Selectivity estimation
│   └── mod.rs                 # Module exports
│
├── format.rs                  # Formatting utilities for IR components
└── mod.rs                     # Main module exports
```

## Components

### Expression System (`expr/`)

The expression system provides the fundamental building blocks for representing query plans:

- **s_expr.rs**: Implements `SExpr` (Single Expression), which is a tree of relational operators. Each `SExpr` contains a plan (relational operator), children, and metadata like applied rules and properties. It represents a complete query plan tree.

- **m_expr.rs**: Implements `MExpr` (Multiple Expression), which is the representation of relational expressions inside the `Memo`. Each `MExpr` references its parent group, contains a plan operator, and references child groups rather than direct child expressions.

- **extract.rs**: Provides pattern matching and extraction utilities for analyzing and transforming expressions during optimization.

Logical and physical trees own different operator types. A single `ExprKind`
contract supplies each stage's operator/state, cache access, rewrite invalidation
and validation dispatch. Logical and physical kinds implement it independently;
shared tree utilities do not convert payloads into a cross-stage inspection enum:

- Display depends on `OperatorHumanizer<O>`, with the concrete `IdHumanizer` type
  selected by its generic method. Logical and physical dispatch remain separate,
  and payload formatting helpers use static dispatch.
  Default tree formatting has concrete SExpr/PExpr entry points. Trace collection
  consumes formatted text, retaining logical-to-physical transitions without extra
  expression capability traits or lifetime bounds.
- Type and scope validation have stage-specific entries and dispatch. The Final /
  Partial aggregate input protocol exists only in physical scope validation.
- Binding-time Scan column updates belong to SExpr. Execution-time Udf columns,
  Exchange inspection and lazy-materialization checks belong to PExpr. These
  helpers inspect their own stage's enum directly, without inspection traits.
- `Matcher` inspects only operator kind and tree shape through `Operator`, not
  payloads. Logical rules and physical passes share structural patterns; Memo
  extraction still accepts only logical expressions.

`MemoOperator` remains a tagged storage identity, not a shared payload view used
by tree consumers.

### Physical Implementation Boundary

`implementation.rs` enumerates local physical fragments for one logical expression.
A fragment references the expression's children by input position; it contains no
Memo group IDs, selected child plans, logical rule history, or property caches.
`PhysicalImplementations` names a default implementation explicitly and includes
all alternatives. The current strategies remain two-phase aggregation and direct
implementations for other operators.

There are two consumers of the same candidate definitions:

- `expr/physical_plan.rs`: `DefaultPhysicalPlanner` chooses the explicit default
  recursively for fallback, skipped search, and direct planning. Only equivalent
  roots may share logical semantic caches; intermediate nodes start fresh.
- `memo.rs`: after logical exploration, all implementation roots enter the
  original equivalence group. Intermediate nodes get independently derived
  properties/statistics and are interned by their operator and resolved inputs.
  Logical expressions record enumeration completion; physical expressions do
  not have an enumeration state. Generation is independent of required physical
  properties and completes before costing or reusing a cached best choice.

This is a local fragment description, not a third complete plan IR or a logical
rewrite rule. Distribution enforcers remain a separate Memo insertion path.

### Memo Structure (`memo.rs`)

The `Memo` is a central data structure that efficiently stores and manages equivalent expressions:

- Organizes expressions into logical equivalence groups
- Maintains a lookup table to detect duplicate expressions
- Provides methods for inserting, retrieving, and manipulating expressions
- Tracks the best implementation for each required property

In `EXPLAIN MEMO` output, logical expressions are prefixed with `~` (for example
`#0 ~Join [#1, #2]`). Unprefixed expressions are physical; only they are costed
and referenced by best properties. Physical aggregates show their phase, as in
`Aggregate(Final)`. A group holding only `Aggregate(Partial)` is the intermediate
input of a two-phase aggregate rather than a logical equivalence group.

### Group System (`group.rs`)

The `Group` represents a set of logically equivalent relational expressions:

- Contains multiple `MExpr` instances that are logically equivalent
- Maintains shared relational properties for all expressions in the group
- Tracks optimization state (init, explored) for search algorithms
- Maps required properties to the best expression implementation

### Property System (`property/`)

The property system manages plan properties and requirements:

- **property.rs**: Defines core property types like `RelationalProperty`, `PhysicalProperty`, and `RequiredProperty`
- **enforcer.rs**: Provides property enforcers to satisfy physical requirements
- **builder.rs**: Borrows concrete SExpr/PExpr trees and Memo contexts for property
  derivation, without erased tree-access traits. Tree child properties use their
  own caches; Memo children use group semantics or selected physical properties.

### Statistics System (`stats/`)

The statistics system provides components for statistical analysis and cost estimation:

- **column_stat.rs**: Implements column-level statistics for data distribution analysis
- **histogram.rs**: Provides histogram-based statistics for more detailed data distribution
- **selectivity.rs**: Contains utilities for estimating predicate selectivity

## Usage

The IR components in this directory are used by various optimizer implementations:

- **Cascades Optimizer**: Uses the memo-based approach for cost-based optimization
- **Recursive Optimizer**: Applies transformation rules recursively
- **HyperDP Optimizer**: Implements dynamic programming for join ordering

These components provide the foundation for rule-based transformations and cost-based optimization in Databend's query processing pipeline.
