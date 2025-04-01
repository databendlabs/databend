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

### Memo Structure (`memo.rs`)

The `Memo` is a central data structure that efficiently stores and manages equivalent expressions:

- Organizes expressions into logical equivalence groups
- Maintains a lookup table to detect duplicate expressions
- Provides methods for inserting, retrieving, and manipulating expressions
- Tracks the best implementation for each required property

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
- **builder.rs**: Provides utilities for building and deriving properties

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
