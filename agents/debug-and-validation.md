# Debug Guidelines

- For Rust changes that will remain in the branch, use `cargo clippy` to confirm there are no compilation or lint errors.
- Start with partial verification when a full workspace pass is too expensive, but move toward stronger coverage before handoff when the resulting changes will remain in the branch.
- Use the trace-debug utilities under [`src/query/service/tests/it/trace_debug/README.md`](../src/query/service/tests/it/trace_debug/README.md) when the question is about query lifecycle or stage transitions, such as parser/planner/interpreter/pipeline flow, HTTP query state and pagination behavior, or which execution spans and attached events appear for a specific SQL path.
- For temporary investigation outputs that will not be submitted, keep validation minimal and purpose-driven. Run only the checks needed to establish the conclusion or unblock the investigation.
- If investigation work begins producing code, tests, or docs that should remain in the branch, raise the validation bar before handoff.

# Testing Guidelines

## Changes Intended to Remain in the Branch

- Keep unit tests close to the affected crate with `#[cfg(test)]` modules.
- Put integration behavior in the relevant SQL suites or meta harnesses such as `tests/metactl` and `tests/meta-kvapi`.
- Every planner, executor, or storage change should add at least one regression SQL file plus expected output when deterministic.
- Use cluster variants such as `make stateless-cluster-test` and TLS mode when coordination, transactions, or auth are involved.
- Document new fixtures or configs in `tests/README.md` or inline comments so CI stays reproducible.

## Temporary Investigation Outputs

- Do not apply the full submission-level test bar by default.
- Run only the tests needed to establish the conclusion, compare options, or unblock the investigation.
- If temporary investigation output is being converted into a real change for submission, switch to the branch-retained change test bar before handoff.
