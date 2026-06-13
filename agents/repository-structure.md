# Repository Structure & Module Organization

Databend is a Rust workspace rooted at `Cargo.toml`. Start by deciding whether the change belongs to the query engine or the metadata system, then read the closest module README before editing.

## Start Here: Meta vs Query

- `src/query/` is the main query engine: SQL parsing, planning, optimization, expression evaluation, execution pipeline, query service integration, and table/storage-facing behavior all converge here. If the task changes SQL behavior, planner output, execution, storage access, or query-side tests, start with `src/query/README.md`.
- `src/meta/` is the metadata side of the system: metadata types, protocol definitions, compatibility layers, supporting tooling, and the Databend meta-service binaries live here. If the task touches catalog/schema metadata, protobuf compatibility, meta APIs, or `databend-meta` tooling, start with `src/meta/README.md`.
- `src/meta/README.md` also notes an important boundary: core meta-service implementation has moved to the separate `databend-meta` repository. Read it early if you suspect the code you need is no longer in this workspace.

## Query Module Guide

- Use `src/query/README.md` as the index for query-side crates. It explains the role of modules such as `ast`, `catalog`, `expression`, `functions`, `pipeline`, `service`, `storages`, and related support crates.
- Prefer the nearest module README when one exists instead of inferring structure from directory names alone.
- Read `src/query/ast/README.md` when the task is about SQL parser structure, grammar internals, or parser-specific development workflow.
- Read `src/query/functions/README.md` when adding or debugging scalar or aggregate functions, function registration, or function-specific tests.
- Read `src/query/ee_features/README.md` when the task involves enterprise query features and you need to confirm which feature layer owns the behavior.
- Read `src/query/sql/README.md` when the task lives in SQL planning, binding, optimizer behavior, or SQL-side planner tests. 

## Other Workspace Landmarks

- Shared crates live in `src/common/`.
- Binaries are stored under `src/binaries/`.
- Tests live in `tests/`, including SQL suites under `tests/suites/`, sqllogic cases, and meta-specific harnesses.
- Performance experiments live in `benchmark/`.
- Tooling and deployment helpers live in `scripts/` and `docker/`.
- Fixtures live in `_data/`.
- The `Makefile` lists supported top-level tasks.
