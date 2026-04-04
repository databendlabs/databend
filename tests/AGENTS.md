# Test Area Guide

- Use this file for work under `tests/`.
- Start by choosing the smallest harness that matches the behavior you need to validate, then read the nearest README for file layout and runner workflow.
- Use `tests/suites/0_stateless/` for stateless SQL regression files and follow its numeric category and sequence naming rules.
- Use `tests/sqllogictests/` for sqllogictest cases and runner-specific workflows such as targeted globs and auto-completion.
- Use `tests/metactl/` when meta type changes require refreshing metactl fixtures or restore flows.
- Use `tests/meta-cluster/`, `tests/compat/`, `tests/compat_fuse/`, and `tests/longrun/` only when the change needs cluster, compatibility, storage-format, or long-running coverage.
- Keep expected outputs deterministic, and document new fixtures or configs when reproducibility depends on them.
