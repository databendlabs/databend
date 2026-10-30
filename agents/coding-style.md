# Coding Style & Naming Conventions

## Rust
- Follow `rustfmt.toml` with 4-space indentation and 100-column width.
- Pass `cargo clippy -- -D warnings`.
- Use `snake_case` for modules and files.
- Use `CamelCase` for exposed types and traits.
- Prefer helpers from `common/exception` for error handling.
- Use `tracing` spans when observability matters.
- Prefer lazy iterator chains, borrowed iteration, and consuming iteration over
  eager `collect()` plus a second pass. Only materialize a `Vec`, `HashSet`, or
  similar collection when an API boundary, ownership requirement, stable order,
  or repeated traversal needs it.
- Avoid cloning shared structures, expressions, plans, and metadata only to
  inspect or filter them. Prefer `iter()`, borrowed helper methods, short-circuit
  iterator methods such as `any`/`all`/`find`, or consuming traversal when the
  caller already owns the value.
- When adding helpers over collections, expose borrowed iterator forms first.
  Add owned collection-returning helpers only when callers genuinely need owned
  materialized results.
- Keep stable identifiers and positions apart. `ColumnId` is a stable id that
  survives projection and schema evolution; a `usize` position into
  `TableSchema.fields`, a `DataBlock`, a bloom index file, or a parquet leaf
  list is not, and neither is a subsystem id (tantivy field id, parquet
  `field_id`). Never bridge them with a cast such as `idx as ColumnId`. When a
  subsystem defines its own ordinal, wrap it in a local newtype without
  `From`/`Deref` to the raw integer (see `BloomIndexColumnOrdinal`), and when
  the subsystem can hand back its own handle, use that instead of recomputing
  the position (see how the inverted index uses the `Field`s returned by
  `create_index_schema`).

## Tests and Fixtures
- Rust unit tests go in a single `#[cfg(test)] mod tests { ... }` block at the
  very end of the source file, after all production code. Never place a test
  module, `#[test]` function, or test-only helper between `impl` blocks or in
  the middle of the implementation. If a file needs more than one test module,
  nest them inside the trailing `mod tests`.
- Name the module `tests`, not `foo_tests` or `request_debug_tests`. Group by
  nested module or by test function name if you need to distinguish topics.
- Before adding a unit test, check whether the crate already has a
  `tests/it/` directory. Most Databend crates keep their tests there; follow
  the existing location and style for that crate instead of introducing an
  inline module where none exists.
- Check whether the behavior is already covered by an existing test before
  adding a new one, and match the assertion and fixture style of nearby tests.
- Keep SQL suite file prefixes consistent with the existing numeric ordering in `tests/suites/`.

## Python
- Python utilities in `tests/` should satisfy Ruff defaults.

## Shell
- Shell scripts should round-trip through `shfmt -l -w`.
