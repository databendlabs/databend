# Repository Guidelines

This file is the top-level guide for repository-specific working rules. Start by confirming the target area and what outputs are expected to remain in the branch. Prefer a closer `AGENTS.md` when one exists, then read the matching detail documents in `agents/`. If maintainer guidance conflicts with repository docs or current practice, treat the maintainer's explicit guidance as the primary source of intent.

## Core Workflow
- Build context from the codebase first. Databend is a multi-crate Rust workspace, so understand the affected module boundaries before editing.
- For code issues inside the repository, default to best-effort root cause analysis. Do not stop at symptom-only fixes when the underlying cause can be found with reasonable investigation.
- When guidance, lower-level docs, and repository practice conflict or are ambiguous, identify the specific conflict that affects the current decision instead of assuming one source is automatically correct.
- Validate incrementally. Run the smallest relevant checks early, and scale verification to the parts that will remain in the branch and enter review.
- Treat tests as part of the change. Planner, executor, storage, and behavior changes should come with regression coverage.
- Keep contributions reviewable. Commits should stay scoped, and pull requests should follow the repository's expected collaboration workflow.

## Submission Intent
- Judge the quality bar by what will remain in the branch and enter review, not by an abstract task label.
- Temporary investigation outputs such as notes, logs, measurements, or ad hoc scripts only need purpose-driven polish, but they should still aim for correct conclusions. If they become branch-retained code, tests, or docs, raise them to the repository's normal review bar before handoff.
- If a relevant repository pattern already exists, follow it without overstating a local pattern as a global hard rule. If no clear pattern exists, call out the gap, request the missing guidance, and use the best task-specific judgment you can defend.
- If constraints force a partial fix, record the constraint and keep the unresolved issue visible.

## Common Commands
- `make build`: build debug binaries for local development.
- `make run-debug`: build and launch a local standalone deployment.
- `make lint`: run formatting, clippy, and repository linters.
- For additional commands and selection guidance, see [`agents/development-commands.md`](agents/development-commands.md).

## Repository Gotchas
- A clean full build of the workspace can take about 20 minutes. Prefer the smallest relevant build or test step first, then scale validation up before handoff.

## Detail Index
- [`agents/repository-structure.md`](agents/repository-structure.md) for workspace layout and where code, tests, tooling, and fixtures live.
- [`agents/development-commands.md`](agents/development-commands.md) for setup, build, run, test, format, and lint commands.
- [`agents/coding-style.md`](agents/coding-style.md) for Rust, Python, shell, naming, error handling, and observability conventions.
- [`agents/debug-and-validation.md`](agents/debug-and-validation.md) for clippy expectations and testing strategy.
- [`agents/commit-and-pr.md`](agents/commit-and-pr.md) for commit format, PR requirements, and the Databend PR template example.
- [`agents/agents-md-alignment.md`](agents/agents-md-alignment.md) for keeping `AGENTS.md` aligned with maintainer intent, repository practice, and lower-level documentation.
- If important development or testing guidance is missing from this index and the linked documents, call out the gap, request the missing guidance, and keep it visible in your summary if it remains unresolved.
