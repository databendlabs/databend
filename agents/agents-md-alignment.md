# Keeping `AGENTS.md` Aligned with Project Reality

Use this document when reviewing or updating `AGENTS.md`. Its job is not to restate repository rules, but to explain how to judge whether `AGENTS.md` still reflects maintainer intent, lower-level docs, and actual repository practice.

## Purpose

- Treat `AGENTS.md` as the top-level operating guide for coding agents in this repository.
- Use `AGENTS.md` for durable direction and routing guidance, not every local detail.
- Revisit `AGENTS.md` when maintainer guidance, supporting docs, or repository practice appear to diverge.

## How to Understand Repository Reality

- The maintainer's explicit guidance is the primary source for intended direction.
- Repository docs and repository practice should normally agree. When they do not, do not assume either side is automatically correct.
- A mismatch may mean the document is outdated, the implementation is wrong, or the current practice is incomplete. Identify the specific conflict first.
- Keep the analysis focused on the part of the mismatch that matters for the current decision. Broaden scope only when the wider context is required to resolve it.

## Document Pyramid

- `AGENTS.md` sits at the top of the pyramid and is written for coding agents.
- `agents/*.md` can add detail and judgment while still serving coding agents first.
- Documents referenced from `agents/*.md` may lean more toward human readers.
- When editing `AGENTS.md`, prefer concise rules and routing guidance over explanations that belong in lower-level documents.

## Alignment Principles

- Prefer root-cause correction over surface-level cleanup.
- Judge the quality bar by what will remain in the branch and enter review.
- Work that will not remain in the branch should still aim for correct conclusions; it just does not automatically require the same submission workflow as branch-retained changes.
- If an established repository pattern exists, follow it.
- If no clear pattern exists, call out the gap, request the missing guidance, and use the best judgment you can defend.
- If constraints force a partial fix, record the constraint and keep the unresolved issue visible.

## What to Check During Review

- Whether `AGENTS.md` still matches maintainer guidance.
- Whether top-level rules in `AGENTS.md` still match `agents/*.md`.
- Whether documented rules still match repository practice.
- Whether `AGENTS.md` is stating a hard rule where the repository only has a local pattern or an unresolved gap.
- Whether `AGENTS.md` is carrying detail that should instead live in `agents/*.md`.

## When You Find Drift

- Record the exact conflict or ambiguity and name the sources in tension: maintainer guidance, `AGENTS.md`, lower-level docs, or repository practice.
- Decide whether the right fix is to update `AGENTS.md`, update lower-level docs, investigate the implementation, or leave a documented gap for follow-up.
- If the missing guidance does not arrive in time, proceed as needed, but keep the gap visible in the final summary.
- Keep the write-up concise and decision-oriented.
