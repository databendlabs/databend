# Merge bump-iceberg with main Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring `bump-iceberg` up to date with `main`, resolve conflicts, and retain the reverted PR #19200 changes before opening a new PR.

**Architecture:** Existing branch `bump-iceberg` already contains the feature; we’ll merge `origin/main` to incorporate the revert and any new commits, then reconcile each conflicted file so the feature logic still applies on top of current sources.

**Tech Stack:** Git, Rust (query engine, settings, fuse storage), gh CLI for PR creation.

### Task 1: Prepare workspace state

**Files:** (none)

1. Verify a clean tree and current branch:
   - Run: `git status -sb`
   - Expected: `## bump-iceberg`
2. Ensure local refs are current:
   - Run: `git fetch origin main`
3. Inspect incoming commits if needed:
   - Run: `git log --oneline --decorate --graph -5 origin/main`

### Task 2: Merge `origin/main` into `bump-iceberg`

**Files:** (merge touches repo-wide)

1. Start the merge while on `bump-iceberg`:
   - Run: `git merge origin/main`
   - Expected: conflict markers in files touched by PR #19200.
2. Note any auto-resolved files but leave them staged until conflicts are settled.

### Task 3: Resolve fuse storage conflicts

**Files:**
- Modify: `src/query/storages/fuse/src/operations/read_partitions.rs`
- Modify: `src/query/storages/fuse/src/operations/read_data.rs`
- Modify: `src/query/storages/fuse/src/fuse_table.rs`

1. Open each file and search for conflict markers (`<<<<<<<`).
2. For `read_partitions.rs`, keep the logic that injects the iceberg bump behavior (as in PR #19200) while rebasing onto any new helper signatures from `main`.
   - Ensure calls into `FuseTable::do_read_data` still pass newly required parameters from `main`.
3. For `read_data.rs`, retain the feature-specific behavior (likely respecting new session settings or storage formats) and harmonize imports or traits added on `main`.
4. For `fuse_table.rs`, ensure any new table methods added on `main` coexist with the iceberg configuration fields; confirm trait impls compile.
5. After each file is fully reconciled, remove conflict markers and stage: `git add <file>`.

### Task 4: Resolve settings and session test conflicts

**Files:**
- Modify: `src/query/settings/src/settings_default.rs`
- Modify: `src/query/service/tests/it/sessions/session_setting.rs`

1. Align the default setting introduced in PR #19200 with whatever defaults now exist on `main`.
   - Preserve the iceberg-related setting name, description, and default, but incorporate any new constants or builder APIs added in `main`.
2. Update the session test to reflect any helper renames or additional assertions now required on `main`.
   - Ensure the test still validates toggling the iceberg setting.
3. Stage both files once conflict markers are removed.

### Task 5: Finalize merge locally

**Files:** all staged conflict resolutions

1. Double-check staged changes:
   - Run: `git status -sb`
2. Build + targeted tests to ensure the feature still compiles:
   - Run: `cargo fmt`
   - Run: `cargo clippy --workspace --all-targets -- -D warnings` (interrupt early if too slow, but attempt)
   - Run: `cargo test -p databend-query session_setting::tests::test_*` (adjust target to cover the modified session test)
3. Complete the merge:
   - Run: `git commit` (use default merge message or customize briefly)

### Task 6: Push and open new PR

**Files:** (none)

1. Push the updated branch:
   - Run: `git push origin bump-iceberg`
2. Create a PR with gh:
   - Run: `gh pr create --base main --head bump-iceberg --title "fix: reapply iceberg bump" --body-file <tmp-file-with-summary>`
   - Body should mention reintroducing PR #19200, summarizing conflicts resolved, and list verification commands run.
3. Capture the PR URL for reporting back to the user.

Plan complete and saved to `docs/plans/2026-01-20-merge-bump-iceberg-with-main.md`. Two execution options:

1. Subagent-Driven (this session) — I handle each task sequentially with reviews.
2. Parallel Session — spin up a new session dedicated to execution using superpowers:executing-plans.

Which approach would you like?
