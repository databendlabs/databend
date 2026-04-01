# Meta Area Guide

- Use this file for work under `src/meta/`.
- Start with [`README.md`](README.md) and confirm whether the task belongs in this workspace or in the separate `databend-meta` repository.
- Route metadata type changes to `app/` or `app-storage/`, protobuf schema changes to `protos/`, compatibility conversion work to `proto-conv/`, KV and API behavior to `api/` or `store/`, and binary entrypoint work to `binaries/`.
- When stored metadata types or protobuf definitions change and the result will remain in the branch, update the related compatibility and version-tracking code together with the type change instead of leaving follow-up work for later.
- Add or refresh compatibility coverage in the relevant meta test suite, especially under `src/meta/proto-conv/tests/it/` when serialization compatibility is involved.
- If the required service-side implementation is no longer in this repository, record that boundary clearly and avoid patching around the missing source.
