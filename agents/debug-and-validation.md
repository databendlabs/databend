# Debug Guidelines

- For Rust changes that will remain in the branch, use `cargo clippy` to confirm there are no compilation or lint errors.
- Start with partial verification when a full workspace pass is too expensive, but move toward stronger coverage before handoff when the resulting changes will remain in the branch.
- For temporary investigation outputs that will not be submitted, keep validation minimal and purpose-driven. Run only the checks needed to establish the conclusion or unblock the investigation.
- If investigation work begins producing code, tests, or docs that should remain in the branch, raise the validation bar before handoff.

# Testing Guidelines

## Changes Intended to Remain in the Branch

- Keep unit tests close to the affected crate with `#[cfg(test)]` modules, placed at the end of the file per [`coding-style.md`](coding-style.md#tests-and-fixtures).
- Put integration behavior in the relevant SQL suites or meta harnesses such as `tests/metactl` and `tests/meta-kvapi`.
- Every planner, executor, or storage change should add at least one regression SQL file plus expected output when deterministic.
- Use cluster variants such as `make stateless-cluster-test` and TLS mode when coordination, transactions, or auth are involved.
- Document new fixtures or configs in `tests/README.md` or inline comments so CI stays reproducible.

## Test admission for AI-assisted changes

Generated tests fail in a specific way: they are easy to produce and hard to reject, because volume looks like coverage. Apply these before a test enters the branch:

- Write one sentence stating which implementation mistake the test would catch; if none, delete the test.
- Expected values come from an independent source (SQL semantics, another engine, a spec, hand computation). A test that re-derives its expectation with the implementation's own logic proves nothing.
- For a bug fix, verify the regression test fails on the parent commit or with the fix reverted; report the command and result in the PR summary. Tests that pass either way are padding.
- One behavior per test. If several inputs are needed, use a table and name the axes; do not copy a test body with different literals.
- Prefer a deterministic SQL logic test for behavior changes; keep unit tests for pure logic that SQL cannot reach. Do not add tests for constructors, constants, or getters.
- A review finding is not a reason to add a test by itself. If the fix is a simplification or a reasoned refusal, no new test is required.

## Temporary Investigation Outputs

- Do not apply the full submission-level test bar by default.
- Run only the tests needed to establish the conclusion, compare options, or unblock the investigation.
- If temporary investigation output is being converted into a real change for submission, switch to the branch-retained change test bar before handoff.
