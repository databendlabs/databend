# Debug Guidelines

- For implementation tasks that touch Rust code, use `cargo clippy` to confirm there are no compilation or lint errors.
- For implementation tasks, start with partial verification when a full workspace pass is too expensive, but move toward full verification before handoff when practical.
- For exploration tasks that do not need to be submitted, keep validation minimal and purpose-driven. Only run checks that are necessary to establish the conclusion or unblock the investigation.
- If an exploration task begins producing branch-worthy code or tests, reclassify it as implementation work and apply the implementation-task validation bar.

# Testing Guidelines

## Implementation Tasks

- Keep unit tests close to the affected crate with `#[cfg(test)]` modules.
- Put integration behavior in the relevant SQL suites or meta harnesses such as `tests/metactl` and `tests/meta-kvapi`.
- Every planner, executor, or storage change should add at least one regression SQL file plus expected output when deterministic.
- Use cluster variants such as `make stateless-cluster-test` and TLS mode when coordination, transactions, or auth are involved.
- Document new fixtures or configs in `tests/README.md` or inline comments so CI stays reproducible.

## Exploration Tasks

- Do not apply the full implementation-task test bar by default.
- Run only the tests needed to establish the conclusion, compare options, or unblock the investigation.
- If the exploration output is being converted into a real change for submission, switch to the implementation-task test bar before handoff.
