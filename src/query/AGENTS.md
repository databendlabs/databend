# Query Area Guide

- Use this file for work under `src/query/`.
- Start with [`README.md`](README.md), then read the nearest module README before editing when one exists.
- Route parser work to `ast/`, function work to `functions/`, planner and optimizer work to `sql/`, execution pipeline work to `pipeline/`, service integration work to `service/`, and table-engine or storage behavior work to `storages/`.
- Read [`sql/README.md`](sql/README.md) for planner structure and optimizer test-support entry points, [`functions/README.md`](functions/README.md) for function registration and tests, and [`ast/README.md`](ast/README.md) for parser-specific workflow.
- Do not infer ownership from directory names alone when a nearby README already defines the boundary.
- Query-side behavior changes that remain in the branch should usually add coverage close to the affected crate and add SQL regression tests under `tests/suites/` or `tests/sqllogictests/` when user-visible SQL behavior changes.

## Code Review Rules

### Fuse Partial UPDATE support boundary

- Review Fuse Partial UPDATE and column-group code only within its documented supported domain.
- Do not report findings whose reproduction requires combining Partial UPDATE with column-oriented
  segments, partitioned tables, stored or virtual computed columns, Inverted/Ngram/Vector/Spatial
  indexes, Fuse virtual columns, or `REPLACE INTO`. These combinations are deliberately
  unsupported; do not require runtime guards, fallbacks, group-aware reads, or derived-metadata
  maintenance unless a change explicitly expands the feature scope.
- Continue to report issues reproducible with supported Partial UPDATE behavior, including ordinary
  Bloom, change tracking, HLL, compression, cluster statistics, metadata compatibility, and
  GC/Vacuum.
