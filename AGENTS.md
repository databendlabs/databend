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

## Testing Guidelines
Unit tests stay close to the affected crate (`#[cfg(test)]` modules), and integration behavior belongs in the relevant SQL suites or meta harness (`tests/metactl`, `tests/meta-kvapi`). Every planner, executor, or storage change should add at least one regression SQL file plus expected output when deterministic. Use cluster variants (`make stateless-cluster-test` and TLS mode) whenever coordination, transactions, or auth are involved. Document new fixtures or configs in `tests/README.md` (or inline comments) so CI remains reproducible.

## Commit & Pull Request Guidelines
History follows a Conventional-style subject such as `fix(storage): avoid stale snapshot (#19174)` or `feat: support self join elimination (#19169)`; keep the first line imperative and under 72 characters. Commits should stay scoped to a logical change set and include formatting/linting updates in the same patch. PRs must outline motivation, implementation notes, and validation commands, plus link issues or RFCs, and the final description should follow `PULL_REQUEST_TEMPLATE.md` (checkboxes, verification, screenshots when needed). Attach screenshots or sample queries when UI, SQL plans, or system tables change, and call out rollout risks (migrations, config toggles, backfills) so reviewers can plan accordingly.
