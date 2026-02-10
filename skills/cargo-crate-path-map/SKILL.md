---
name: cargo-crate-path-map
description: Map file paths in the Databend repo to Cargo crate (package) names, or list crate-to-path mappings for this workspace. Use when asked which crate a file belongs to or to enumerate all crates in this project.
---

# Cargo Crate Path Map

## Overview
Map Databend files or directories to their owning Cargo package and produce a crate-to-path table for this workspace.

## Quick Start
Run from the repo root.
- Map one file:
  python3 <skill_dir>/scripts/crate_path_map.py --workspace-root . --file <path>
- List all crates:
  python3 <skill_dir>/scripts/crate_path_map.py --workspace-root . --list

## Workflow (try multiple methods, pick the best)
1. Use the Databend workspace root.
   - Prefer `cargo metadata` because it returns `workspace_root`.
   - If Cargo is unavailable, search upward from the file for a `Cargo.toml` with `[workspace]`.
2. Method A (best, authoritative): `cargo metadata --no-deps --format-version 1`.
   - Use `--method metadata` to force this path.
   - This respects workspace members, excludes, and path overrides.
3. Method B (fallback, no Cargo): scan `Cargo.toml` files and parse `[package].name`.
   - Use `--method scan`.
   - Skip workspace-only manifests without `[package]`.
4. Resolve a file path by choosing the package whose `manifest_dir` is the longest prefix of the file path.

## Output Guidance
- For single files: `path -> crate_name (manifest_dir)`.
- For lists: table with `crate_name`, `manifest_dir`, `manifest_path`.
- When using fallbacks, note the method and any uncertainty.

## Pitfalls
- Workspace root `Cargo.toml` may not define `[package]`; ignore it.
- `cargo metadata` paths are absolute; normalize before comparing.
- Exclude `target/` and `.git/` when scanning.
