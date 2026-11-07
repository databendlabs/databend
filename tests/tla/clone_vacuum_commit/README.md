# CloneVacuumCommit

Models an explicit transaction that writes to a table, concurrent with `CREATE TABLE ... CLONE` and `VACUUM` on the same table. The safety question: can VACUUM ever delete a snapshot, segment or block that a published table head or a bound clone still references?

| | |
|---|---|
| Bound source | databendlabs/databend `29b08e94e01f0a3462815a4628ce05fac02a7b2f` |
| Source review | 2026-11-05, explicitly requested current-code audit: no source drift; tracked code clean, only `tests/tla/` untracked. Refined the atomic secondary-writer abstraction; corrected the GC-cache scope description. |
| Generated with | evot `tla-model` skill `7ec32cf` (same runner as the verified run), TLC 1.8.0 |
| Last TLC run | 2026-11-05: 41/41 checks (24 exhaustive normal, 5 mutations, 12 witnesses); 26,253,217 summed normal distinct states |
| Executed model | SHA-256: TLA `719bb360929af12d36ce1f7d7c9666b2c2ce04ad81309e0cd7f9d9bdc30d7ddd`; manifest `a30bcddc43a0dcd99b74e1901d5f69072e31a77966547df230fd81f4570cde67` |

Default runs replay this model for the bound commit; they do not scan current changes or auto-correct it. Only an explicit current-branch/PR/source-consistency request authorizes synchronization and rebinding.

## Modeled logic
- Secondary writer: atomic baseline or split metadata read, time sample, block/segment/snapshot writes, version/LVT precheck and CAS; one attempt without retry. `sink_commit.rs::async_process`, `append_generator.rs::do_generate_new_snapshot`, `table_api.rs::update_multi_table_meta_with_sender`
- Transaction writer: buffered append or overwrite statements, commit CAS, conflict retry with the txn-begin timestamp check. `session_catalog.rs::get_table`, `query_ctx/table.rs::get_table_meta_timestamps_impl`, `snapshot_generator.rs::decorate_snapshot`, `retry/commit.rs::try_rebuild_req`, `table_api.rs::update_multi_table_meta_with_sender`
- CLONE: AT SNAPSHOT navigation, source-version refresh, staged binding with LVT check, anchor write and install, failure. `navigate.rs::find_snapshot_with_version`, `interpreter_table_create.rs::create_table_clone`, `table_api.rs::build_create_table_clone_txn`
- VACUUM: owner fence, GC root under time or snapshot-count retention, clone descendant mark, LVT publish and rescan, then block, segment and snapshot deletion. `vacuum_table_v2.rs::vacuum_base_snapshot_phase`, `vacuum_table_v2.rs::do_vacuum2`, `vacuum.rs::prepare_snapshot_gc_selection`, `vacuum.rs::select_gc_root`, `vacuum.rs::mark_clone_descendants`, `table_api.rs::set_table_lvts`

## Safety properties
- `PublishedReferencesExist`: the source head and the bound clone head exist, with every segment and block they reference.
- `TransactionHistorySkipsBufferedSnapshots`: a later buffered statement's snapshot points to the transaction base, not to an earlier buffered snapshot.
- `BufferedFenceIsMinimum`: a grouped transaction keeps the earliest buffered snapshot time as its fence.
- `OriginalObjectTimePreserved`: a retry keeps the original block timestamp.
- `TypeOK`: variables stay within their bounded domains.

Bounds and abstractions are in `model.json` `scope`, the action-to-code map in `source_map`, and resolved model errors in `audit`. Not covered: concurrent/repeated GC, clone writes, multi-table commits, pagination, partial-delete errors, DROP/FLASHBACK/tags. MAINTENANCE still uses memory cache; this is not a multi-GC stale-cache proof.

The bound source is the audited revision, not necessarily this checkout's HEAD or the PR base. Results are bounded model checks, not a proof of the Rust implementation. No new native replay was performed for the 41-case audit.

## Run
```bash
python3 ~/.evotai/skills/tla-model/scripts/tlc_run.py tests/tla/clone_vacuum_commit
# Requires Python 3.10+ and Java 11+; use --java /path/to/java if needed.
```
