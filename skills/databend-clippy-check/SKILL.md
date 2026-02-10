---
name: databend-clippy-check
description: Run a basic compilation/style check for Databend changes. Use after modifying one or more workspace crates to decide whether the change is local or global, then run `cargo clippy` (prefer `--fix`) at the right scope, or run `make lint` for global changes.
---

# Databend Clippy Check

## Overview
Decide whether a change is local or global based on crate dependents, then run `cargo clippy -- -D warnings` (prefer `--fix`) at the appropriate scope.

## Workflow
1. Identify the modified crate(s).
   - Use recent edits, `git status`, or `git diff` to find touched crate paths.
2. Find dependents of each modified crate.
   - Prefer `cargo tree -i -p <crate>` to list reverse dependencies.
   - If the output is huge or slow, limit depth or inspect only major dependents first.
3. Classify impact.
   - If dependent crates are many (or include core crates), treat as global.
   - If unclear, ask for a threshold and explain the count.
4. Run clippy.
   - Local change: start with the modified crate, then its dependents (top-down or jump to key ones).
   - Global change: run `make lint`.
   - Prefer `cargo clippy --fix` when allowed, then re-run without `--fix` to confirm.
5. Adapt to runtime/output.
   - If clippy is too noisy or slow, shrink scope, or run `-p <crate>` only.
   - If clippy hangs, try a smaller subset before escalating scope.
6. Capture real runtime results to guide future strategy.
   - Record crate, command, wall time, and whether it was slow/noisy.
   - Use the record to decide which crates to skip until the final pass.

## Command Examples
Local:
- `cargo clippy -p <crate> --fix --allow-dirty --allow-staged -- -D warnings`
- `cargo clippy -p <crate> -- -D warnings`

Global:
- `make lint`

## Notes
- Keep the workflow interactive; do not assume a fixed threshold.
- Report the scope decision and why.
- Maintain a lightweight, committable log at `skills/databend-clippy-check/clippy_runtime.md`.

## Runtime Log (best practice)
- Location: `skills/databend-clippy-check/clippy_runtime.md` (committable, concise).
- Content: a single Markdown table.
  - First column: crate name.
  - Remaining columns: short descriptions (e.g., rough dependency reach, known slow/fast behavior, or skip-until-final guidance).
  - Do not paste verbose clippy output; keep entries stable across runs.
