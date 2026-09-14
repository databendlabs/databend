# Optimizer Guide

This file applies to optimizer work under this directory. Follow the parent
query and SQL-area guidance for general development and validation rules.

## Rule Testing

- Use [`eager_aggregation.rs`](../../../tests/it/optimizer/eager_aggregation.rs)
  as the reference pattern for optimizer rule tests.
- Start rule behavior tests from SQL. Bind the SQL into the raw plan, run the
  production optimizer pipeline, and record both the raw and optimized plans
  in a module-local golden file.
- Treat the raw plan as part of the test evidence: it confirms that preceding
  optimizer phases in the current codebase can actually produce the pattern
  consumed by the rule.
- Review the complete optimized-plan golden. A rule test should cover the
  intended rewrite together with its interaction with subsequent rules, not
  only assert the presence of one operator.
- Include positive and negative SQL cases when a rewrite has semantic
  preconditions. Give each case a name and description that state the intended
  optimizer outcome.
- If changes to preceding phases make an intermediate pattern unreachable,
  revise or reconsider the SQL case. Do not preserve an obsolete pattern by
  replacing the test with a manually constructed `SExpr`.
- Do not use manually constructed complete `SExpr` trees as the primary
  evidence for rule correctness. Direct Rust unit tests remain appropriate for
  pure helpers, scalar-expression rewrites, column-set calculations, and local
  invariants that do not depend on optimizer phase ordering.
- Use the [shared optimizer replay data](../../../test-support/data/README.md)
  when a case needs reusable table fixtures, statistics, or physical-plan
  output. Add SQL logic test coverage when the rewrite affects user-visible
  results, output columns, or NULL semantics.
