# Databend SQL

`src/query/sql/` is the query-side SQL crate (`databend-common-sql`). This is the place to start when a change is about SQL binding, logical planning, optimizer behavior, or SQL-side planner test infrastructure.

## Module Guide

- [`src/planner/`](./src/planner/) contains the binder, planner, optimizer, and related plan representations. Start here for most SQL semantics and optimizer tasks.
- [`src/executor/`](./src/executor/) contains SQL-side execution helpers that sit close to planning output.
- [`src/evaluator/`](./src/evaluator/) contains evaluation logic used by the SQL crate.

## Tests and Test Support

- Prefer the `test-support` toolchain when you need to understand or debug SQL-layer behavior. It is the fastest way to replay planner/optimizer cases, inspect shared fixtures, and reason about SQL-side regressions before moving to heavier service-side debugging.
- Prefer golden-file comparisons of the full output over asserting on a few extracted features from that output. Full-output snapshots make SQL-layer behavior changes easier to inspect, review, and debug.
- [`tests/it/`](./tests/it/) contains integration tests for the SQL crate, including the lightweight planner replay coverage.
- [`test-support/`](./test-support/) contains shared SQL test-support utilities and fixtures used by planner/optimizer tests.
- Read [`test-support/data/README.md`](./test-support/data/README.md) when you need to answer any of these questions:
  - where a shared optimizer replay case, statistics fixture, table definition, or generated golden file should live;
  - why both `src/query/sql/tests/it/planner.rs` and `src/query/service/tests/it/sql/planner/optimizer/optimizer_test.rs` changed after one optimizer-case edit;
  - how to add a new shared optimizer case or update an existing one;
  - why a shared optimizer case is not being discovered, filtered, or regenerated as expected.
