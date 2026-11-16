# Commit & Pull Request Guidelines

## Commits
- Use a Conventional-style subject such as `fix(storage): avoid stale snapshot (#19174)` or `feat: support self join elimination (#19169)`.
- Keep the first line imperative and under 72 characters.
- Keep each commit scoped to one logical change set.
- Include formatting and linting updates in the same patch when they belong to the change.

## Pull Requests
- Allowed PR title types are `rfc`, `feat`, `fix`, `refactor`, `ci`, `docs`, and `chore`; prefer `ci` for test-only or CI-only changes.
- Optional PR title scopes may contain only lowercase letters, digits, and hyphens, such as `query` or `admin-status`.
- Follow `PULL_REQUEST_TEMPLATE.md` without changing its structure:
  - Put everything you want to say under `## Summary`: motivation, key changes, test coverage, linked issues or RFCs, risks. Keep it short.
  - Do not add new sections (`## Changes`, `## Implementation`, ...).
  - In `## Tests` and `## Type of change`, only tick checkboxes. Do not add, remove, or reword items, and do not append text below the list. Describe test files and cases in Summary, including the reason when ticking `No Test`.
  - In `## AI assistance`, only fill in the existing fields.
- In Summary, attach screenshots or sample queries when UI, SQL plans, or system tables change, and call out rollout risks such as migrations, config toggles, or backfills.
- Push the branch to your fork and create the PR into `origin`.
- You can use `gh` tooling for the PR flow.
- Agent-opened PRs must fill in the "AI assistance" section of the PR template, including the responsible human (`@github-id`) — the author-side owner who has read the diff and can explain the changes during review. See [`AI_POLICY.md`](../AI_POLICY.md).
- Before opening or updating a PR, present the full diff to the responsible human for reading. Do not request review on code no human has read.
- Hold the automatic Dev CI while iterating on review feedback (e.g., Codex): `gh pr edit <PR> --add-label hold-ci` (applies from the next push; fast checks and review triggers still run; opt-in label workflows such as `ci-cloud` are unaffected).
- Whenever you add or remove `hold-ci`, say so in your reply to the responsible human: which PR, that heavy CI is paused (so skipped jobs and a failing `ready` check are expected), and that you will remove it once review converges.
- Resume full CI once review converges, without a new commit: `gh pr edit <PR> --remove-label hold-ci`, then re-run the Dev run for the PR head: `gh run rerun $(gh run list -w Dev -c $(gh pr view <PR> --json headRefOid -q .headRefOid) -L1 --json databaseId -q '.[0].databaseId')`.
- Mark the PR ready for human review (`gh pr ready <PR>`) only after review converges and full CI passes.

## Example PR Description

```md
I hereby agree to the terms of the CLA available at: https://docs.databend.com/dev/policies/cla/

## Summary

- Allow table functions such as `generate_series` and `range` to take scalar subqueries as arguments, e.g. `generate_series((SELECT count() FROM numbers(10))::int, 39)`.
- `bind_table_args` tries constant folding first, then falls back to executing the subquery; empty results return NULL, matching scalar subquery semantics.
- Tests: `02_0063_function_generate_series.test` covers `generate_series` and `range` with subquery arguments.

## Tests

- [ ] Unit Test
- [x] Logic Test
- [ ] Benchmark Test
- [ ] No Test - _Explain why_

## Type of change

- [ ] Bug Fix (non-breaking change which fixes an issue)
- [x] New Feature (non-breaking change which adds functionality)
- [ ] Breaking Change (fix or feature that could cause existing functionality not to work as expected)
- [ ] Documentation Update
- [ ] Refactoring
- [ ] Performance Improvement
- [ ] Other (please describe):

## AI assistance

- AI usage: An AI coding agent drafted the implementation; I reviewed the final diff and added logic tests
- Responsible human: @your-actual-github-id
- [x] The responsible human has read every line of this diff and can explain each change

<!-- Reviewable:start -->

---

This change is [<img src="https://reviewable.io/review_button.svg" height="34" align="absmiddle" alt="Reviewable"/>](https://reviewable.io/reviews/databendlabs/databend/19213)

<!-- Reviewable:end -->
```
