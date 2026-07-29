# Databend AI Policy

Databend welcomes AI-assisted development.

We already ship agent-oriented product features and maintain repository guidance for coding agents (`AGENTS.md`, `agents/`). This policy is about **how humans and AI collaborate on contributions** so maintainers can keep the warehouse correct, reviewable, and safe.

This policy covers contributions to this repository: code, tests, docs, issues, PR descriptions, and review discussion.

## Principles

1. **AI is a normal engineering tool.** Using models, agents, or IDE assistants is allowed and encouraged when it improves quality or speed.
2. **A human remains accountable.** Whoever opens the PR owns the change: correctness, security, compatibility, tests, and follow-up fixes.
3. **Understanding is mandatory.** If you cannot explain the change in your own words, it is not ready to submit.
4. **Reviewer time is scarce.** Low-effort AI output that shifts the real work onto maintainers will usually be closed.
5. **Database semantics need evidence.** Planner, executor, storage, meta, and SQL behavior changes need tests that can falsify the implementation — not vibes.

## What is allowed

You may use AI to:

- Explore the codebase and form a root-cause hypothesis
- Draft implementations, refactors, tests, docs, and benchmarks
- Generate alternative designs for a human to compare
- Help with formatting, boilerplate, and mechanical edits
- Assist local review before you open a PR

When working with coding agents in this repository, follow `AGENTS.md` and the linked docs under `agents/`. Prefer the smallest relevant build/test loop, keep diffs reviewable, and treat tests as part of the change.

## What is not allowed

The following will usually result in the issue/PR being closed, possibly without extended discussion:

1. **Ownerless autonomous contribution**
   - Agents **may** open PRs, update branches, and use `gh` tooling — see `agents/commit-and-pr.md`. What is banned is a PR with no accountable human: every PR must name an author-side person who has read the diff, can explain the changes, will answer review questions, and owns follow-up fixes. This person is not the reviewer — reviewers are assigned separately via CODEOWNERS
   - Bulk drive-by PRs that show no local validation or understanding of Databend module boundaries

2. **AI-generated conversation as a substitute for thinking**
   - Pasting model replies as your response to maintainer questions
   - Long, generic, high-confidence commentary that does not address the concrete code or failure mode
   - Using AI to argue for a change you cannot defend yourself
   - Exception: using AI to translate or polish **your own** reasoning (for example, writing in Chinese and posting an AI-assisted English version) is fine and encouraged — the ideas must be yours

3. **Slop submissions**
   - PRs that do not compile, fail basic lint, or clearly were not read by the author
   - Fake or tautological tests that only mirror the implementation
   - Huge unrelated diffs, noisy renames, or “cleanup” mixed into a behavior change without need
   - Secret redaction failures, license-incompatible pasted code, or unexplained dependency churn

4. **Bypassing collaboration norms**
   - Skipping issue/RFC discussion for large semantic or architectural changes
   - Ignoring CLA, PR template, CODEOWNERS review paths, or requested test evidence

## Human accountability requirements

Agent-opened PRs are welcome. Every PR — however it was produced — must have a **responsible human** (the PR author, or the person named in the PR body for bot-authored PRs) who has:

- **Read every line of the final diff.** Unread code does not enter review. If the diff is too large to read, it is too large to submit — split it

and who can:

- State the user-visible or developer-visible problem in plain language
- Explain why this approach is correct for Databend’s architecture
- Point to the tests or manual validation that would fail if the fix were wrong
- Answer review questions without outsourcing the reply to a model transcript
- Own production risk: compatibility, upgrade/migration, performance, and data safety

If no responsible human engages when maintainers ask, the PR may be closed regardless of code quality.

AI assistance does **not** lower the quality bar. It raises the author’s duty to filter bad output before review.

## Self-review before requesting review

Generated code fails in patterns that hand-written code rarely does. Before requesting review, the responsible human should walk the diff specifically looking for:

- **Hallucinated interfaces**: calls to functions, settings, or SQL behaviors that do not exist in this codebase or behave differently than the model assumed
- **Plausible-but-wrong edge cases**: NULL handling, overflow, empty input, timezone/precision, non-UTF-8 — verify against actual Databend behavior, not the model’s memory of “how databases work”
- **Tests that mirror the implementation**: a test that re-derives expected output from the same logic proves nothing; expected values should come from an independent source (MySQL/other engines, a spec, or hand computation)
- **Silently swallowed errors**: `unwrap_or_default`, broad `match _ =>`, or dropped `Result`s inserted to make code compile
- **Unnecessary abstraction and dead code**: traits, generics, helpers, and config knobs that no second caller needs; delete them before review
- **Comments and names that don’t match behavior**: generated comments often describe intent, not what the code does
- **Divergence from neighboring code**: if the surrounding module solves the same problem differently, follow it or explain why not

Fixing these before review is the author’s job. Finding them in review is a signal the diff was not read.

## Disclosure (recommended)

Disclosure is **recommended**, not a merge blocker by default.

Add a short note in the PR body when AI materially helped with:

- Code generation or substantial edits
- Tests or docs
- PR summary text for a complex change

Examples:

```md
## AI assistance

- Used Claude Code for the initial storage iterator patch; I reworked error paths and added logic tests myself.
- Used an LLM to draft the PR summary; technical claims were verified against the diff.
```

```md
## AI assistance

IDE autocomplete only. No agent-generated patches.
```

You do **not** need to disclose ordinary autocomplete, spell check, or linter suggestions.

Maintainers may ask for disclosure or a walkthrough when a change is high risk (catalog/meta, storage correctness, transactions, planner semantics, security boundaries). Failure to disclose is not automatic rejection; refusal to take responsibility is.

## Quality bar for AI-assisted PRs

AI-assisted changes must still satisfy normal Databend contribution expectations:

### Scope
- Prefer small, reviewable PRs over agent-produced megadiffs
- Split mechanical regeneration from semantic changes
- Do not mix drive-by refactors into bug fixes

### Evidence
- Bug fixes: include a regression test whenever practical
- Planner / executor / storage behavior: add or update logic tests (or the relevant suite) with expected output when deterministic
- Performance claims: include before/after measurements, not anecdotes
- “No Test — Explain why” in the PR template requires a real reason, not convenience

### Local validation
- Run the smallest relevant checks first, then stronger validation before handoff
- For Rust changes intended to land, do not submit known clippy/format failures
- If full workspace validation is expensive, say what you ran and what remains uncovered

### Communication
- Write PR summaries for humans: motivation, behavior change, risks, validation
- Quote model output only when needed, inside blockquotes, with your own interpretation
- Keep discussion concrete and tied to files, tests, and failure modes

## Maintainer and reviewer guidance

Review the change, not the tool.

- **At least one human must have read the diff before merge.** Approvals from AI review bots do not count toward this; they are assistants, not reviewers
- Spend human attention on correctness, data safety, compatibility, performance, and test adequacy
- Prefer questions that check author understanding over style nits already handled by CI. For high-risk areas, one probing question (“why is this branch safe when the snapshot is stale?”) reveals more than ten nits
- Read the tests first: verify they can fail, and that expected outputs come from an independent source rather than the implementation itself
- If a PR looks AI-generated and low-effort, ask for a tighter summary, tests, or a reduced diff; close it if the author cannot engage
- Domain CODEOWNERS remain the authority for their areas
- It is fine to use AI to help review, but merge decisions stay with humans

## Security, license, and confidentiality

- You are responsible for ensuring contributed material is license-compatible, whether typed or generated
- Do not paste proprietary code, private customer data, credentials, or internal secrets into prompts in a way that causes them to land in the repository
- Treat security-sensitive areas (auth, authorization, multi-tenant isolation, sandbox UDF boundaries) as high scrutiny regardless of how the code was produced

## Relationship to other docs

| Doc | Role |
| --- | --- |
| `AI_POLICY.md` (this file) | Community rules for AI-assisted contribution and review |
| `AGENTS.md` + `agents/` | How coding agents should work inside this repository |
| `.github/PULL_REQUEST_TEMPLATE.md` | PR checklist (CLA, summary, tests, change type) |
| `.github/CODEOWNERS` | Domain reviewers for merge paths |

If maintainer guidance for a specific PR conflicts with this document, follow the maintainer’s explicit guidance for that PR, then propose an update here if the exception should become policy.

## Enforcement

- Maintainers may request changes, ask for human clarification, require more tests, or close PRs that violate this policy
- Repeated low-effort or unattended agent spam may result in blocked contributions
- Good-faith AI-assisted work that is well explained, tested, and reviewable is welcome

## Short version

Use AI freely.  
Read every line before you submit.  
Submit only what you understand, can test, and can defend.  
Do not waste reviewer time with unattended slop.
