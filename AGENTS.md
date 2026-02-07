# Repository Guidelines

## Project Structure & Module Organization
Databend is a Rust workspace rooted at `Cargo.toml`. The query engine (`src/query/`), meta service (`src/meta/`), and shared crates (`src/common/`) compile into binaries stored under `src/binaries/`. Tests live in `tests/` with SQL suites (`suites/`), sqllogic cases, and meta-specific harnesses, while performance experiments sit in `benchmark/`. Tooling and deployment helpers are collected in `scripts/` and `docker/`, fixtures in `_data/`, and the `Makefile` lists every supported task.

## Build, Test, and Development Commands
- `make setup` – install cargo components plus linters (taplo, shfmt, typos, machete, ruff).
- `make build` / `make build-release` – compile debug or optimized `databend-{query,meta,metactl}` into `target/`.
- `make run-debug` / `make kill` – launch or stop a local standalone deployment using the latest build.
- `make unit-test`, `make stateless-test`, `make sqllogic-test`, `make metactl-test` – run focused suites; `make test` expands to the default CI matrix.
- `make fmt` and `make lint` – enforce formatting and clippy checks before committing.

## Coding Style & Naming Conventions
Rust sources follow `rustfmt.toml` (4-space indent, 100-column width) and must pass `cargo clippy -- -D warnings`. Keep modules and files in `snake_case`, exposed types and traits in `CamelCase`, and SQL suite files prefixed with the numeric order already in `tests/suites/`. Favor `common/exception` helpers plus `tracing` spans for errors and observability. Python utilities in `tests/` should satisfy Ruff defaults, and shell scripts must round-trip through `shfmt -l -w`.

## Debug Guidelines
Always use `cargo clippy` to make sure there are no compilation errors. Fully verifying the whole project may take too long, partial verification can speed things up, but eventually a full verification will be needed.

## Testing Guidelines
Unit tests stay close to the affected crate (`#[cfg(test)]` modules), and integration behavior belongs in the relevant SQL suites or meta harness (`tests/metactl`, `tests/meta-kvapi`). Every planner, executor, or storage change should add at least one regression SQL file plus expected output when deterministic. Use cluster variants (`make stateless-cluster-test` and TLS mode) whenever coordination, transactions, or auth are involved. Document new fixtures or configs in `tests/README.md` (or inline comments) so CI remains reproducible.


## Commit & Pull Request Guidelines

History follows a Conventional-style subject such as `fix(storage): avoid stale snapshot (#19174)` or `feat: support self join elimination (#19169)`; keep the first line imperative and under 72 characters. Commits should stay scoped to a logical change set and include formatting/linting updates in the same patch. PRs must outline motivation, implementation notes, and validation commands, plus link issues or RFCs, and the final description should follow `PULL_REQUEST_TEMPLATE.md` (checkboxes, verification, screenshots when needed). Attach screenshots or sample queries when UI, SQL plans, or system tables change, and call out rollout risks (migrations, config toggles, backfills) so reviewers can plan accordingly.

There is the example of pull requests:

````
I hereby agree to the terms of the CLA available at: https://docs.databend.com/dev/policies/cla/

## Summary

- Enable table functions like `generate_series` and `range` to accept scalar subqueries as arguments
- Return NULL for empty scalar subqueries to align with existing scalar subquery semantics

## Changes

This PR enables SQL like:

```sql
SELECT generate_series AS install_date
FROM generate_series(
    (SELECT count() FROM numbers(10))::int,
    (SELECT count() FROM numbers(39))::int
);
````

Previously, table function arguments could only be constants. Now they can be scalar subqueries that return a single value.

## Implementation

1. Added `contains_subquery()` function to detect subqueries in AST expressions
2. Added `execute_subquery_for_scalar()` to execute and extract scalar values from subqueries
3. Modified `bind_table_args` to try constant folding first, then fall back to subquery execution
4. The subquery executor is passed from the binder to enable runtime evaluation
5. Returns `Scalar::Null` for empty subquery results (aligns with LeftSingleJoin behavior)

## Tests

- [x] Unit Test
- [x] Logic Test
- [ ] Benchmark Test
- [ ] No Test - Pair with the reviewer to explain why

Added tests in `02_0063_function_generate_series.test` for:

- `generate_series` with subquery arguments
- `range` with subquery arguments

## Type of change

- [x] New feature (non-breaking change which adds functionality)

<!-- Reviewable:start -->

---

This change is [<img src="https://reviewable.io/review_button.svg" height="34" align="absmiddle" alt="Reviewable"/>](https://reviewable.io/reviews/databendlabs/databend/19213)

<!-- Reviewable:end -->

```


Pull request mus be pushed into fork and create pr into origin.
You can use gh tools to do it.
```

