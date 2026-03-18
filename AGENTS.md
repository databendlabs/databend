# Repository Guidelines

This file is the entry point for repository-specific working rules. Keep it focused on overall direction; read the matching detail documents in `agents/` before making changes in that area.

## Core Workflow
- Build context from the codebase first. Databend is a multi-crate Rust workspace, so understand the affected module boundaries before editing.
- Validate incrementally. Run the smallest relevant checks early, and apply the verification standard that matches the task type.
- Treat tests as part of the change. Planner, executor, storage, and behavior changes should come with regression coverage.
- Keep contributions reviewable. Commits should stay scoped, and pull requests should be pushed to your fork and opened against `origin`.

## Task Types
- Implementation tasks: the result is intended to stay in the branch, be reviewed, or be submitted. Follow the normal quality bar for edits, validation, tests, commits, and PR readiness.
- Exploration tasks: the result is mainly temporary, used for investigation, debugging, measurement, or option evaluation, and is not intended to be submitted as-is. Prioritize speed and useful conclusions over polish.
- If the output you plan to keep is only notes, logs, ad hoc scripts, or temporary experiments, treat it as exploration work.
- If the output includes code, tests, or docs that are expected to remain in the branch for review or submission, treat it as implementation work.
- For exploration tasks that do not produce submit-worthy changes, avoid spending time on low-value checks, exhaustive formatting passes, or broad test runs unless they are needed to answer the question correctly.
- If a task starts as exploration and turns into a real code change that should be kept, switch back to the implementation-task standard before handoff.

## Detail Index
- [`agents/repository-structure.md`](agents/repository-structure.md) for workspace layout and where code, tests, tooling, and fixtures live.
- [`agents/development-commands.md`](agents/development-commands.md) for setup, build, run, test, format, and lint commands.
- [`agents/coding-style.md`](agents/coding-style.md) for Rust, Python, shell, naming, error handling, and observability conventions.
- [`agents/debug-and-validation.md`](agents/debug-and-validation.md) for clippy expectations and testing strategy.
- [`agents/commit-and-pr.md`](agents/commit-and-pr.md) for commit format, PR requirements, and the Databend PR template example.
- If you discover high-value information that would materially help development or testing but cannot be found through this Detail Index and the linked documents beneath it, call that gap out explicitly instead of assuming the current documentation path is sufficient.

## Default Rule
If guidance in a detail file is relevant to the task, follow it. Determine the task type first, then apply the matching validation and testing rules. Exploration-task guidance is an explicit exception to the default implementation-task quality bar.
