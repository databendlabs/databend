# Commit & Pull Request Guidelines

## Commits
- Use a Conventional-style subject such as `fix(storage): avoid stale snapshot (#19174)` or `feat: support self join elimination (#19169)`.
- Keep the first line imperative and under 72 characters.
- Keep each commit scoped to one logical change set.
- Include formatting and linting updates in the same patch when they belong to the change.

## Pull Requests
- Allowed PR title types are `rfc`, `feat`, `fix`, `refactor`, `ci`, `docs`, and `chore`; prefer `ci` for test-only or CI-only changes.
- Optional PR title scopes may contain only lowercase letters, digits, and hyphens, such as `query` or `admin-status`.
- Outline motivation, implementation notes, and validation commands.
- Link issues or RFCs when relevant.
- Follow `PULL_REQUEST_TEMPLATE.md`, including checkboxes, verification, and screenshots when needed.
- Attach screenshots or sample queries when UI, SQL plans, or system tables change.
- Call out rollout risks such as migrations, config toggles, or backfills.
- Push the branch to your fork and create the PR into `origin`.
- You can use `gh` tooling for the PR flow.
- Agent-opened PRs must fill in the "AI assistance" section of the PR template, including the responsible human (`@github-id`) — the author-side owner who has read the diff and can explain the changes during review. See [`AI_POLICY.md`](../AI_POLICY.md).
- Before opening or updating a PR, present the full diff to the responsible human for reading. Do not request review on code no human has read.

## Example PR Description

```md
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
```

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

## AI assistance

- AI usage: An AI coding agent drafted the implementation; I reviewed the final diff and added logic tests
- Responsible human: @your-actual-github-id
- [x] The responsible human has read every line of this diff and can explain each change

<!-- Reviewable:start -->

---

This change is [<img src="https://reviewable.io/review_button.svg" height="34" align="absmiddle" alt="Reviewable"/>](https://reviewable.io/reviews/databendlabs/databend/19213)

<!-- Reviewable:end -->
```
