# Coding Style & Naming Conventions

## Rust
- Follow `rustfmt.toml` with 4-space indentation and 100-column width.
- Pass `cargo clippy -- -D warnings`.
- Use `snake_case` for modules and files.
- Use `CamelCase` for exposed types and traits.
- Prefer helpers from `common/exception` for error handling.
- Use `tracing` spans when observability matters.

## Tests and Fixtures
- Keep SQL suite file prefixes consistent with the existing numeric ordering in `tests/suites/`.

## Python
- Python utilities in `tests/` should satisfy Ruff defaults.

## Shell
- Shell scripts should round-trip through `shfmt -l -w`.
