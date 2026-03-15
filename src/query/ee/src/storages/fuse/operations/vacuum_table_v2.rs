// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Logs from this module will show up as "[FUSE-VACUUM2] ...".
databend_common_tracing::register_module_tag!("[FUSE-VACUUM2]");

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::TimeDelta;
use chrono::Utc;
use databend_common_catalog::plan::block_id_from_location;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::GcDroppedTableBranchReq;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::RetentionPolicy;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ASSUMPTION_MAX_TXN_DURATION;
use databend_common_storages_fuse::operations::SnapshotGcSelection;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::meta::is_uuid_v7;
use databend_storages_common_table_meta::table::OPT_KEY_REFERENCED_BRANCH_IDS;
use futures::StreamExt;
use futures::TryStreamExt;
use log::info;
use log::warn;
use opendal::Entry;
use opendal::ErrorKind;

use super::common::StoragePrefixes;
use super::common::table_id_by_path;

/// Segments that must be kept alive during cleanup, grouped by the table that stores them.
type ProtectedSegmentsByTable = HashMap<u64, HashSet<Location>>;

/// Block IDs (as i128) that must be kept alive during cleanup, grouped by the table that stores them.
type ProtectedBlocksByTable = HashMap<u64, HashSet<i128>>;

/// Staged snapshot-based branches are first persisted under a hidden dropped orphan name and only
/// become visible after commit_table_branch_meta() publishes the real branch name. Keep them out
/// of immediate final GC for a short grace period so a concurrent vacuum2 does not reclaim them
/// before the final commit finishes.
const STAGED_ORPHAN_GC_GRACE_PERIOD: TimeDelta = TimeDelta::minutes(10);

/// GC state for a base table or branch after selecting the gc root.
struct BranchGcState {
    table_id: u64,
    /// The snapshot-embedded timestamp of the gc root (from `TableSnapshot::timestamp`).
    gc_root_snapshot_ts: DateTime<Utc>,
    /// The object-storage `last_modified` timestamp of the gc root file.
    /// Used with `ASSUMPTION_MAX_TXN_DURATION` to safely filter old-version objects.
    gc_root_snapshot_meta_ts: DateTime<Utc>,
    protected_segments: HashSet<Location>,
}

/// Phase B result for a single branch.
enum BranchPhaseResult {
    /// Has gc root with snapshots to clean.
    NeedsCleanup {
        state: BranchGcState,
        snapshot_files_to_gc: Vec<String>,
    },
    /// No cleanup needed. Carries the gc root snapshot if one was found (for segment protection).
    ///
    /// By design we use the selected gc root, not the earliest historical snapshot, as the
    /// protection boundary for this branch. Once `prepare_snapshot_gc_selection()` selects a
    /// gc root, vacuum2 also advances the table LVT to that boundary; snapshots older than the
    /// gc root may still exist physically for a while, but are intentionally treated as no longer
    /// reachable by the current branch history contract. Therefore cross-table protection only
    /// needs to inspect the gc root snapshot here.
    NoCleanup(Option<Arc<TableSnapshot>>),
}

/// Vacuum a table and all its history branches.
#[async_backtrace::framed]
pub async fn do_vacuum2(
    table: &dyn Table,
    ctx: Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Vec<String>> {
    let table_info = table.get_table_info();
    if ctx.txn_mgr().lock().is_active() {
        info!(
            "Transaction is active, skipping vacuum, target table {}",
            table_info.desc
        );
        return Ok(vec![]);
    }

    let now = Utc::now();
    let retention_boundary =
        now - Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);

    // Step 1: select and clean the base table if it has a gc root.
    let fuse_table = FuseTable::try_from_table(table)?;
    let latest_snapshot = fuse_table.read_table_snapshot().await?;
    let (base_gc_state, base_snapshot_files) = if let Some(latest_snapshot) = latest_snapshot {
        vacuum_base_table(fuse_table, &ctx, latest_snapshot, respect_flash_back).await?
    } else {
        (None, vec![])
    };

    let mut files_to_gc: Vec<String> = base_snapshot_files;
    let mut storage_prefixes: StoragePrefixes = HashMap::from([(
        format!("{}/", fuse_table.meta_location_generator().prefix()),
        fuse_table.get_id(),
    )]);

    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let history_branches = catalog
        .list_history_table_branches(ListHistoryTableBranchesReq {
            table_id: fuse_table.get_id(),
            retention_boundary: None,
        })
        .await?;
    let s3_storage_class = ctx.get_settings().get_s3_storage_class()?;

    // Step 2: classify branch history and select gc roots.
    // Phase A (serial): construct branch tables, handle expiry, classify beyond-retention.
    // Phase B (parallel): select gc roots for retainable candidates.
    let mut beyond_retention_branches: Vec<Box<FuseTable>> = Vec::new();
    let mut gc_root_candidates: Vec<Box<FuseTable>> = Vec::new();
    for branch in history_branches {
        if ctx.check_aborting().is_err() {
            info!("vacuum2 aborted by user");
            return Ok(files_to_gc);
        }
        let branch_id = branch.branch_id.table_id;
        let expire_at = branch.expire_at;
        let drop_on = branch.branch_meta.data.drop_on;
        let branch_name = branch.branch_name.clone();
        let branch_table = fuse_table.branch_table_from_meta(branch, &s3_storage_class)?;

        let storage_prefix = format!("{}/", branch_table.meta_location_generator().prefix());
        storage_prefixes.insert(storage_prefix, branch_id);

        if drop_on.is_some_and(|drop_on| {
            drop_on < retention_boundary
                && (!branch_name.starts_with("orphan@")
                    || drop_on + STAGED_ORPHAN_GC_GRACE_PERIOD < now)
        }) {
            beyond_retention_branches.push(branch_table);
            continue;
        }

        if expire_at.is_some_and(|expire_at| expire_at <= now) {
            // After soft-drop, the branch still needs to go through gc_root selection
            // and respect the retention period before its data can be cleaned up.
            if let Err(err) = catalog
                .drop_table_branch(DropTableBranchReq {
                    tenant: ctx.get_tenant(),
                    table_id: fuse_table.get_id(),
                    branch_name,
                    branch_id: branch_table.get_id(),
                })
                .await
            {
                warn!(
                    "drop expired branch failed, ignored, branch: {}, err: {}",
                    branch_table.get_table_info().desc,
                    err
                );
            }
        }

        gc_root_candidates.push(branch_table);
    }

    // Phase B (parallel): select gc roots, cleanup snapshots, and classify — all in one pass.
    let concurrency = ctx.get_settings().get_max_threads()? as usize;
    let gc_results = futures::stream::iter(gc_root_candidates.into_iter().map(|branch_table| {
        let ctx = ctx.clone();
        async move {
            let gc_selection =
                if let Some(latest_snapshot) = branch_table.read_table_snapshot().await? {
                    let latest_snapshot_loc = branch_table.snapshot_loc().unwrap();
                    prepare_snapshot_gc_selection(
                        branch_table.as_ref(),
                        &ctx,
                        latest_snapshot,
                        &latest_snapshot_loc,
                        respect_flash_back,
                    )
                    .await?
                } else {
                    None
                };
            let Some(gc_selection) = gc_selection else {
                return Ok::<_, ErrorCode>((branch_table, BranchPhaseResult::NoCleanup(None)));
            };
            let snapshot_files_to_gc = gc_selection.snapshots_to_gc;
            if snapshot_files_to_gc.is_empty() {
                // Keep using the selected gc root as the protection boundary.
                // See `BranchPhaseResult::NoCleanup` for the LVT-based contract behind this:
                // snapshots older than the gc root are considered no longer reachable for
                // branch-history protection, even if some old snapshot files are still present.
                return Ok((
                    branch_table,
                    BranchPhaseResult::NoCleanup(Some(gc_selection.gc_root)),
                ));
            }
            let gc_root_snapshot_ts = gc_selection.gc_root.timestamp.ok_or_else(|| {
                ErrorCode::IllegalReference(format!(
                    "Table {} snapshot lacks required timestamp",
                    branch_table.get_table_info().desc
                ))
            })?;
            let gc_root_snapshot_meta_ts = gc_selection.gc_root_meta_ts;
            let protected_segments: HashSet<Location> =
                gc_selection.gc_root.segments.iter().cloned().collect();
            branch_table
                .cleanup_snapshot_files(&ctx, &snapshot_files_to_gc, false)
                .await?;
            let gc_state = BranchGcState {
                table_id: branch_table.get_id(),
                gc_root_snapshot_ts,
                gc_root_snapshot_meta_ts,
                protected_segments,
            };
            Ok((branch_table, BranchPhaseResult::NeedsCleanup {
                state: gc_state,
                snapshot_files_to_gc,
            }))
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;

    let mut retainable_with_gc_root = Vec::new();
    let mut retainable_without_gc_root = Vec::new();
    for (branch_table, result) in gc_results {
        match result {
            BranchPhaseResult::NeedsCleanup {
                state,
                snapshot_files_to_gc,
            } => {
                files_to_gc.extend(snapshot_files_to_gc);
                retainable_with_gc_root.push((branch_table, state));
            }
            BranchPhaseResult::NoCleanup(gc_root_snapshot) => {
                retainable_without_gc_root.push((branch_table, gc_root_snapshot));
            }
        }
    }

    // Step 3: determine which tables are actually at risk in this round. Non-cleanup branches
    // only need to protect these tables because they are the only ones that may be cleaned.
    let tables_at_risk: HashSet<u64> = base_gc_state
        .as_ref()
        .map(|s| s.table_id)
        .into_iter()
        .chain(retainable_with_gc_root.iter().map(|(_, s)| s.table_id))
        .chain(beyond_retention_branches.iter().map(|b| b.get_id()))
        .collect();
    if tables_at_risk.is_empty() {
        info!(
            "Table {} has no base cleanup, no retainable branch cleanup, and no final branch gc; stopping vacuum",
            table_info.desc
        );
        return Ok(files_to_gc);
    }

    // Step 4: merge all protected segments that must survive cross-table cleanup.
    // GC-root tables contribute their protected segments directly.
    // Non-cleanup branches contribute via collect_external_segments (parallel):
    //   - external segments: stored under at-risk tables, protected as segments.
    //   - self-segments: stored under the branch itself, scanned in Step 5 for cross-table
    //     block references (e.g. after compact on a branch derived from another branch).
    let mut protected_segments_by_table: ProtectedSegmentsByTable = HashMap::new();
    let mut self_segments_to_scan = Vec::new();
    let gc_root_segments = base_gc_state
        .as_ref()
        .into_iter()
        .chain(retainable_with_gc_root.iter().map(|(_, state)| state));
    for gc_state in gc_root_segments {
        for segment in gc_state.protected_segments.iter() {
            let tid = table_id_by_path(&storage_prefixes, &segment.0)?;
            protected_segments_by_table
                .entry(tid)
                .or_default()
                .insert(segment.clone());
        }
    }
    // Parallel: collect external segments from non-cleanup branches.
    // Branches with a gc root snapshot pass it directly to skip find_earliest_snapshot_via_history.
    let external_segments_results =
        futures::stream::iter(retainable_without_gc_root.into_iter().map(
            |(branch_table, gc_root_snapshot)| {
                let storage_prefixes = &storage_prefixes;
                let tables_at_risk = &tables_at_risk;
                async move {
                    collect_external_segments(
                        branch_table.as_ref(),
                        storage_prefixes,
                        tables_at_risk,
                        gc_root_snapshot,
                    )
                    .await
                }
            },
        ))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    for result in external_segments_results {
        let (segments_by_table, self_segments) = result?;
        for (tid, segments) in segments_by_table {
            protected_segments_by_table
                .entry(tid)
                .or_default()
                .extend(segments);
        }
        self_segments_to_scan.extend(self_segments);
    }

    // Step 5: expand protected segments into protected blocks.
    // Streams segments with bounded concurrency via read_compact_segment + buffer_unordered
    // to avoid holding all CompactSegmentInfo in memory. Only blocks belonging to at-risk
    // tables are tracked — other tables won't be cleaned this round so their blocks need no
    // protection.
    let all_segments = protected_segments_by_table
        .values()
        .flat_map(|segs| segs.iter().cloned())
        .chain(self_segments_to_scan)
        .collect::<Vec<_>>();
    let op = fuse_table.get_operator();
    // `read_compact_segment()` takes a schema only for legacy V0/V1 segment migration.
    // In this branch cleanup flow, old segments can only come from the base table lineage, so
    // the base table schema is the correct compatibility schema to use. V2+ segments do not
    // depend on the supplied schema, and reading old base-owned segments with the current evolved
    // base schema is also the expected migration path.
    let schema = fuse_table.schema();
    let mut protected_blocks_by_table: ProtectedBlocksByTable = HashMap::new();
    let mut segment_stream = futures::stream::iter(all_segments.into_iter().map(|segment_loc| {
        let op = op.clone();
        let schema = schema.clone();
        async move { SegmentsIO::read_compact_segment(op, segment_loc, schema, false).await }
    }))
    .buffer_unordered(concurrency);
    while let Some(segment) = segment_stream.next().await {
        for block in segment?.block_metas()? {
            let block_id = block_id_from_location(&block.location.0)?;
            let tid = table_id_by_path(&storage_prefixes, &block.location.0)?;
            if !tables_at_risk.contains(&tid) {
                continue;
            }
            protected_blocks_by_table
                .entry(tid)
                .or_default()
                .insert(block_id);
        }
    }

    // Step 6: final-gc beyond-retention branches that no longer own protected data.
    let mut gc_pending_branches = Vec::new();
    let mut final_gc_branches = Vec::new();
    for branch_table in beyond_retention_branches {
        let protected_segments = protected_segments_by_table
            .remove(&branch_table.get_id())
            .unwrap_or_default();
        let protected_blocks = protected_blocks_by_table
            .remove(&branch_table.get_id())
            .unwrap_or_default();

        if protected_segments.is_empty() && protected_blocks.is_empty() {
            final_gc_branches.push(branch_table);
        } else {
            gc_pending_branches.push(branch_table);
        }
    }
    let base_table_id = fuse_table.get_id();
    let final_gc_results =
        futures::stream::iter(final_gc_branches.into_iter().map(|branch_table| {
            let ctx = ctx.clone();
            async move {
                let branch_name = branch_table.get_table_info().name.clone();
                final_gc_branch(
                    &ctx,
                    branch_table.as_ref(),
                    base_table_id,
                    &branch_name,
                    retention_boundary,
                )
                .await
            }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    for result in final_gc_results {
        files_to_gc.extend(result?);
    }

    // Step 7: clean table-local segment/block files in parallel for all tables with cleanup work.
    // Build a unified cleanup target list: (table_ref, table_id, cutoff, needs_snapshot_sweep).
    let mut cleanup_targets = Vec::new();
    if let Some(ref base_gc_state) = base_gc_state {
        cleanup_targets.push((
            fuse_table,
            base_gc_state.table_id,
            Some((
                base_gc_state.gc_root_snapshot_ts,
                base_gc_state.gc_root_snapshot_meta_ts,
            )),
            false,
        ));
    }
    for (branch_table, gc_state) in &retainable_with_gc_root {
        cleanup_targets.push((
            branch_table.as_ref(),
            gc_state.table_id,
            Some((
                gc_state.gc_root_snapshot_ts,
                gc_state.gc_root_snapshot_meta_ts,
            )),
            false,
        ));
    }
    for branch_table in &gc_pending_branches {
        cleanup_targets.push((branch_table.as_ref(), branch_table.get_id(), None, true));
    }

    type CleanupFut<'a> = futures::future::BoxFuture<'a, Result<Vec<String>>>;
    let cleanup_futs: Vec<CleanupFut<'_>> = cleanup_targets
        .into_iter()
        .map(|(table, tid, cutoff, needs_snapshot_sweep)| {
            let ctx = ctx.clone();
            let protected_segment_paths = protected_segments_by_table
                .remove(&tid)
                .unwrap_or_default()
                .into_iter()
                .map(|(p, _)| p)
                .collect::<HashSet<_>>();
            let protected_blocks = protected_blocks_by_table.remove(&tid).unwrap_or_default();
            Box::pin(async move {
                let snapshots_to_gc = if needs_snapshot_sweep {
                    table
                        .list_files_for_gc(
                            table.meta_location_generator().snapshot_location_prefix(),
                            None,
                        )
                        .await?
                } else {
                    vec![]
                };
                cleanup_table_data(
                    table,
                    ctx,
                    cutoff,
                    snapshots_to_gc,
                    protected_segment_paths,
                    protected_blocks,
                )
                .await
            }) as CleanupFut<'_>
        })
        .collect();

    let cleanup_results = futures::stream::iter(cleanup_futs)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    for result in cleanup_results {
        files_to_gc.extend(result?);
    }

    Ok(files_to_gc)
}

#[async_backtrace::framed]
async fn vacuum_base_table(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    latest_snapshot: Arc<TableSnapshot>,
    respect_flash_back: bool,
) -> Result<(Option<BranchGcState>, Vec<String>)> {
    let latest_snapshot_loc = fuse_table.snapshot_loc().unwrap();
    let Some(gc_root) = prepare_snapshot_gc_selection(
        fuse_table,
        ctx,
        latest_snapshot,
        &latest_snapshot_loc,
        respect_flash_back,
    )
    .await?
    else {
        return Ok((None, vec![]));
    };

    let mut protected_segments = gc_root
        .gc_root
        .segments
        .iter()
        .cloned()
        .collect::<HashSet<_>>();

    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let tags = catalog
        .list_table_tags(ListTableTagsReq {
            table_id: fuse_table.get_id(),
            include_expired: true,
        })
        .await?;

    let mut protected_snapshot_locs = HashSet::new();
    for (tag_name, seq_tag) in tags {
        if seq_tag
            .data
            .expire_at
            .is_some_and(|expire_at| expire_at <= Utc::now())
        {
            if let Err(err) = catalog
                .drop_table_tag(DropTableTagReq {
                    table_id: fuse_table.get_id(),
                    tag_name,
                    seq: MatchSeq::Exact(seq_tag.seq),
                })
                .await
            {
                warn!(
                    "drop expired tag failed, ignored, table: {}, err: {}",
                    fuse_table.get_table_info().desc,
                    err
                );
            }
            continue;
        }

        protected_snapshot_locs.insert(seq_tag.data.snapshot_loc.clone());
        // Snapshot paths encode timestamps via UUID v7, so lexicographic `<` means
        // chronologically before the gc root — only those older snapshots need protection.
        if seq_tag.data.snapshot_loc < gc_root.gc_root_path {
            if let Some(snapshot) = SnapshotsIO::read_snapshot_for_vacuum(
                fuse_table.get_operator(),
                &seq_tag.data.snapshot_loc,
            )
            .await?
            {
                protected_segments.extend(snapshot.segments.iter().cloned());
                // Protect the table_statistics_location referenced by the tag snapshot.
                if let Some(ref stats_loc) = snapshot.table_statistics_location {
                    protected_snapshot_locs.insert(stats_loc.clone());
                }
            }
        }
    }

    // Remove snapshots that are protected by active tags from the gc list.
    let mut snapshot_files_to_gc = gc_root.snapshots_to_gc;
    snapshot_files_to_gc.retain(|path| !protected_snapshot_locs.contains(path));
    if snapshot_files_to_gc.is_empty() {
        // All snapshot candidates before the gc root are protected by active tags.
        // Intentionally stop here instead of continuing to sweep table-local garbage files:
        // vacuum2 is allowed to under-clean but must not risk reclaiming anything that might
        // still be reachable through retained tag history. This matches the conservative design
        // goal for protected snapshots in the branch/tag cleanup path.
        return Ok((None, vec![]));
    }

    fuse_table
        .cleanup_snapshot_files(ctx, &snapshot_files_to_gc, false)
        .await?;
    let gc_root_snapshot_ts = gc_root.gc_root.timestamp.ok_or_else(|| {
        ErrorCode::IllegalReference(format!(
            "Table {} snapshot lacks required timestamp",
            fuse_table.get_table_info().desc
        ))
    })?;

    Ok((
        Some(BranchGcState {
            table_id: fuse_table.get_id(),
            gc_root_snapshot_ts,
            gc_root_snapshot_meta_ts: gc_root.gc_root_meta_ts,
            protected_segments,
        }),
        snapshot_files_to_gc,
    ))
}

/// Collect segments from a non-cleanup branch that reference at-risk tables.
///
/// If `gc_root_snapshot` is provided (branch has a gc root but nothing to clean), it is used
/// directly. Otherwise, `find_earliest_snapshot_via_history` is called to locate the snapshot.
///
/// Safety: only the gc_root (or earliest) snapshot needs to be examined. After branch creation,
/// all new segments and blocks are written under the branch's own storage prefix. Cross-table
/// segment references are inherited solely from the source snapshot at creation time. Subsequent
/// operations (insert, compact, etc.) never introduce new external segment references, so no
/// intermediate snapshot can reference an external segment that the gc_root does not.
///
/// Returns two parts:
/// - `ProtectedSegmentsByTable`: segments stored under *other* at-risk tables that must be protected.
/// - `Vec<Location>`: self-segments (stored under this branch) that may reference blocks in
///   at-risk tables after compaction — scanned for block-level protection in Step 5.
#[async_backtrace::framed]
async fn collect_external_segments(
    branch_table: &FuseTable,
    storage_prefixes: &StoragePrefixes,
    tables_at_risk: &HashSet<u64>,
    gc_root_snapshot: Option<Arc<TableSnapshot>>,
) -> Result<(ProtectedSegmentsByTable, Vec<Location>)> {
    if tables_at_risk.is_empty() {
        return Ok((HashMap::new(), vec![]));
    }

    // Branch has no snapshot at all — nothing to protect.
    if gc_root_snapshot.is_none() && branch_table.snapshot_loc().is_none() {
        return Ok((HashMap::new(), vec![]));
    }

    // Fast pre-check: if the branch records which other branches it references, and none of
    // them are at risk, we can skip the expensive snapshot chain traversal entirely.
    // Branches created before this option was introduced fall through to the full traversal.
    if let Some(ref_ids_str) = branch_table
        .get_table_info()
        .meta
        .options
        .get(OPT_KEY_REFERENCED_BRANCH_IDS)
    {
        let has_at_risk_ref = ref_ids_str
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse::<u64>().ok())
            .any(|id| tables_at_risk.contains(&id));
        if !has_at_risk_ref {
            return Ok((HashMap::new(), vec![]));
        }
    }

    let snapshot = match gc_root_snapshot {
        Some(snap) => snap,
        None => {
            let Some(snap) = branch_table.find_earliest_snapshot_via_history().await? else {
                return Ok((HashMap::new(), vec![]));
            };
            snap
        }
    };

    let current_table_id = branch_table.get_id();
    let mut protected_segments_by_table: ProtectedSegmentsByTable = HashMap::new();
    let mut self_segments: Vec<Location> = Vec::new();
    for segment in &snapshot.segments {
        let tid = table_id_by_path(storage_prefixes, &segment.0)?;
        if tid == current_table_id {
            self_segments.push(segment.clone());
            continue;
        }
        if !tables_at_risk.contains(&tid) {
            continue;
        }
        protected_segments_by_table
            .entry(tid)
            .or_default()
            .insert(segment.clone());
    }
    Ok((protected_segments_by_table, self_segments))
}

/// Clean segments and blocks under a single table's storage prefix.
///
/// `cutoff` is `Some((snapshot_ts, meta_ts))` when a gc root exists — files newer than the
/// cutoff are not listed. `None` means list all files (used for gc-pending branches).
/// `snapshots_to_gc` is typically empty here because snapshot files are already cleaned
/// in Steps 1/2; it is non-empty only for gc-pending branches that need a full sweep.
#[async_backtrace::framed]
async fn cleanup_table_data(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    cutoff: Option<(DateTime<Utc>, DateTime<Utc>)>,
    snapshots_to_gc: Vec<String>,
    protected_segment_paths: HashSet<String>,
    protected_blocks: HashSet<i128>,
) -> Result<Vec<String>> {
    // Cleanup is always table-local: list candidate files under this table, then filter out the
    // segments and blocks that must stay alive for other tables.
    let table_info = fuse_table.get_table_info();
    let segments_to_gc: Vec<_> = fuse_table
        .list_files_for_gc(
            fuse_table
                .meta_location_generator()
                .segment_location_prefix(),
            cutoff,
        )
        .await?
        .into_iter()
        .filter(|path| !protected_segment_paths.contains(path))
        .collect();
    let mut blocks_to_gc = Vec::new();
    for path in fuse_table
        .list_files_for_gc(
            fuse_table.meta_location_generator().block_location_prefix(),
            cutoff,
        )
        .await?
    {
        let block_id = match block_id_from_location(&path) {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    "skip block with unparsable UUID during cleanup, path: {}, err: {}",
                    path, err
                );
                continue;
            }
        };
        if !protected_blocks.contains(&block_id) {
            blocks_to_gc.push(path);
        }
    }

    let files_to_gc = cleanup_table_files(
        fuse_table,
        ctx,
        snapshots_to_gc,
        segments_to_gc,
        blocks_to_gc,
    )
    .await?;
    info!(
        "vacuum2 table cleaned, table: {}, files: {}",
        table_info.desc,
        files_to_gc.len()
    );
    Ok(files_to_gc)
}

#[async_backtrace::framed]
async fn final_gc_branch(
    ctx: &Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    base_table_id: u64,
    branch_name: &str,
    retention_boundary: DateTime<Utc>,
) -> Result<Vec<String>> {
    // Final-gc deletes both the branch kv metadata and the entire branch storage directory once
    // no protected table-local data remains.
    let table_info = fuse_table.get_table_info();
    let branch_dir = format!(
        "{}/",
        FuseTable::parse_storage_prefix_from_table_info(table_info)?
    );

    // Delete storage first: remove_all is idempotent, so if metadata deletion fails later,
    // the next vacuum retry will re-attempt both steps safely.
    if let Err(err) = fuse_table.get_operator().remove_all(&branch_dir).await {
        warn!(
            "cleanup non-retainable branch data failed, ignored, table: {}, branch: {}, err: {}",
            table_info.desc, branch_name, err
        );
        return Ok(vec![]);
    }

    let catalog = ctx.get_catalog(table_info.catalog()).await?;
    if let Err(err) = catalog
        .gc_drop_table_branch(GcDroppedTableBranchReq {
            tenant: ctx.get_tenant(),
            table_id: base_table_id,
            branch_name: branch_name.to_string(),
            branch_id: fuse_table.get_id(),
            retention_boundary,
        })
        .await
    {
        // Only beyond-retention branches reach final GC, and undrop is retention-gated in
        // meta API, so logging here is sufficient while the next vacuum run can retry cleanup.
        warn!(
            "gc non-retainable branch kvs failed, ignored, table: {}, branch: {}, err: {}",
            table_info.desc, branch_name, err
        );
    }
    Ok(vec![branch_dir])
}

#[async_backtrace::framed]
async fn cleanup_table_files(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    snapshots_to_gc: Vec<String>,
    segments_to_gc: Vec<String>,
    blocks_to_gc: Vec<String>,
) -> Result<Vec<String>> {
    let op = Files::create(ctx.clone(), fuse_table.get_operator());
    // Companion files are derived from blocks/segments, so deleting them first keeps the data-file
    // deletion order aligned with the rest of vacuum2.
    let mut stats_to_gc = segments_to_gc
        .iter()
        .map(|path| {
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(path)
        })
        .collect::<Vec<_>>();
    let stats_gc_count = stats_to_gc.len();
    op.remove_file_in_batch(&std::mem::take(&mut stats_to_gc))
        .await?;

    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &fuse_table.get_table_info().meta.indexes;
    let indexes_per_block = table_agg_index_ids.len() + inverted_indexes.len() + 2;

    // TODO: index_paths pre-allocates blocks_to_gc * indexes_per_block strings, which can be
    // very large for tables with millions of blocks and many indexes. Consider batched deletion
    // if OOM becomes an issue.
    let mut index_paths = Vec::with_capacity(blocks_to_gc.len() * indexes_per_block);
    for loc in &blocks_to_gc {
        for index_id in &table_agg_index_ids {
            index_paths.push(
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                ),
            );
        }
        for idx in inverted_indexes.values() {
            index_paths.push(
                TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                    loc,
                    idx.name.as_str(),
                    idx.version.as_str(),
                ),
            );
        }
        index_paths
            .push(TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc));
        // vacuum by refresh virtual column.
        // index_paths.push(TableMetaLocationGenerator::gen_virtual_block_location(loc));
    }
    let indexes_gc_count = index_paths.len();
    op.remove_file_in_batch(&std::mem::take(&mut index_paths))
        .await?;

    let subject_files_to_gc = segments_to_gc
        .into_iter()
        .chain(blocks_to_gc)
        .collect::<Vec<_>>();

    op.remove_file_in_batch(&subject_files_to_gc).await?;
    if !snapshots_to_gc.is_empty() {
        fuse_table
            .cleanup_snapshot_files(&ctx, &snapshots_to_gc, false)
            .await?;
    }

    info!(
        "cleanup_table_files: subject_files={}, snapshots={}, indexes={}, stats_files={}",
        subject_files_to_gc.len(),
        snapshots_to_gc.len(),
        indexes_gc_count,
        stats_gc_count,
    );

    Ok(subject_files_to_gc
        .into_iter()
        .chain(snapshots_to_gc)
        .collect())
}

pub async fn prepare_snapshot_gc_selection(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    latest_snapshot: Arc<TableSnapshot>,
    latest_snapshot_loc: &str,
    respect_flash_back: bool,
) -> Result<Option<SnapshotGcSelection>> {
    let table_info = fuse_table.get_table_info();

    let start = std::time::Instant::now();
    let retention_policy = fuse_table.get_data_retention_policy(ctx.as_ref())?;
    let snapshot_location_prefix = fuse_table
        .meta_location_generator()
        .snapshot_location_prefix();

    let mut is_vacuum_all = false;
    let mut need_update_lvt = false;
    let mut respect_flash_back_with_lvt = None;

    let snapshots_before_lvt = match retention_policy {
        RetentionPolicy::ByTimePeriod(delta_duration) => {
            info!("Using ByTimePeriod policy {:?}", delta_duration);
            let retention_period = if fuse_table.is_transient() {
                // For transient table, keep no history data
                TimeDelta::zero()
            } else {
                delta_duration
            };

            // A zero retention period indicates that we should vacuum all the historical snapshots
            is_vacuum_all = retention_period.is_zero();

            let Some(lvt) = set_lvt(
                latest_snapshot,
                ctx.as_ref(),
                retention_period,
                table_info.ident.table_id,
            )
            .await?
            else {
                return Ok(None);
            };

            if respect_flash_back {
                respect_flash_back_with_lvt = Some(lvt);
            }

            ctx.set_status_info(&format!(
                "Set LVT for table {}, elapsed: {:?}, LVT: {:?}",
                table_info.desc,
                start.elapsed(),
                lvt
            ));

            if is_vacuum_all {
                fuse_table
                    .list_files_until_prefix(
                        snapshot_location_prefix,
                        latest_snapshot_loc,
                        true,
                        None,
                    )
                    .await?
            } else {
                fuse_table
                    .list_files_until_timestamp(snapshot_location_prefix, lvt, true, None)
                    .await?
            }
        }
        RetentionPolicy::ByNumOfSnapshotsToKeep(num_snapshots_to_keep) => {
            info!(
                "Using ByNumOfSnapshotsToKeep policy {:?}",
                num_snapshots_to_keep
            );
            // List the snapshot order by timestamp asc, till the current snapshot(inclusively).
            let need_one_more = true;
            let mut snapshots = fuse_table
                .list_files_until_prefix(
                    snapshot_location_prefix,
                    latest_snapshot_loc,
                    need_one_more,
                    None,
                )
                .await?;
            let len = snapshots.len();
            if len <= num_snapshots_to_keep {
                // Only the current snapshot is there, done
                return Ok(None);
            }
            if num_snapshots_to_keep == 1 {
                // Expecting only one snapshot left, which means that we can use the current snapshot
                // as gc root, this flag will be propagated to the select_gc_root func later.
                is_vacuum_all = true;
            }
            need_update_lvt = true;

            // When selecting the GC root later, the last snapshot in `snapshots` (after truncation)
            // is the candidate, but its commit status is uncertain, so its previous snapshot is used
            // as the GC root instead (except in the is_vacuum_all case).

            // Therefore, during snapshot truncation, we keep 2 extra snapshots;
            // see `select_gc_root` for details.
            let num_candidates = len - num_snapshots_to_keep + 2;
            snapshots.truncate(num_candidates);
            snapshots
        }
    };

    let elapsed = start.elapsed();
    ctx.set_status_info(&format!(
        "Listed snapshots for table {}, elapsed: {:?}, snapshots_dir: {:?}, snapshots: {:?}",
        table_info.desc,
        elapsed,
        snapshot_location_prefix,
        slice_summary(&snapshots_before_lvt)
    ));

    let Some(selection) = select_gc_root(
        fuse_table,
        ctx,
        &snapshots_before_lvt,
        is_vacuum_all,
        respect_flash_back_with_lvt,
    )
    .await?
    else {
        return Ok(None);
    };

    if need_update_lvt {
        let cat = ctx.get_default_catalog()?;
        cat.set_table_lvt(
            &LeastVisibleTimeIdent::new(ctx.get_tenant(), fuse_table.get_id()),
            &LeastVisibleTime::new(selection.gc_root.timestamp.unwrap()),
        )
        .await?;
    }

    ctx.set_status_info(&format!(
        "Selected gc_root for table {}, elapsed: {:?}, gc_root: {:?}, snapshots_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        selection.gc_root,
        slice_summary(&selection.snapshots_to_gc)
    ));
    Ok(Some(selection))
}

/// Select the gc root snapshot from the candidate list.
///
/// The gc root is the dividing line: all snapshots before it are safe to delete.
/// Three strategies are used depending on the retention policy:
/// - `is_vacuum_all`: the current (latest) snapshot is the gc root.
/// - `respect_flash_back`: walk the snapshot chain to find the first snapshot <= LVT.
/// - Otherwise: use the `prev_snapshot_id` of the last candidate as the gc root.
async fn select_gc_root(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    snapshots_before_lvt: &[Entry],
    is_vacuum_all: bool,
    respect_flash_back: Option<DateTime<Utc>>,
) -> Result<Option<SnapshotGcSelection>> {
    let op = fuse_table.get_operator();
    let gc_root_path = if is_vacuum_all {
        // safe to unwrap, or we should have stopped vacuuming in set_lvt()
        fuse_table.snapshot_loc().unwrap()
    } else if let Some(lvt) = respect_flash_back {
        let latest_location = fuse_table.snapshot_loc().unwrap();
        let gc_root = fuse_table
            .find_location(ctx, latest_location, |snapshot| {
                snapshot.timestamp.is_some_and(|ts| ts <= lvt)
            })
            .await
            .ok();
        let Some(gc_root) = gc_root else {
            info!("no gc_root found, stop vacuuming");
            return Ok(None);
        };
        gc_root
    } else {
        if snapshots_before_lvt.is_empty() {
            info!("no snapshots before lvt, stop vacuuming");
            return Ok(None);
        }
        let (anchor, _) = SnapshotsIO::read_snapshot(
            snapshots_before_lvt.last().unwrap().path().to_owned(),
            op.clone(),
            false,
        )
        .await?;
        let Some((gc_root_id, gc_root_ver)) = anchor.prev_snapshot_id else {
            info!("anchor has no prev_snapshot_id, stop vacuuming");
            return Ok(None);
        };
        let gc_root_path = fuse_table
            .meta_location_generator()
            .gen_snapshot_location(&gc_root_id, gc_root_ver)?;
        if !is_uuid_v7(&gc_root_id) {
            info!("gc_root {} is not v7", gc_root_path);
            return Ok(None);
        }
        gc_root_path
    };

    let (stat_result, gc_root) = futures::future::join(
        op.stat(&gc_root_path),
        SnapshotsIO::read_snapshot(gc_root_path.clone(), op.clone(), false),
    )
    .await;
    let gc_root_meta_ts = match stat_result {
        Ok(v) => v.last_modified().ok_or_else(|| {
            ErrorCode::StorageOther(format!(
                "Failed to get `last_modified` metadata of the gc root object '{}'",
                gc_root_path
            ))
        })?,
        Err(e) => {
            return if e.kind() == ErrorKind::NotFound {
                // Concurrent vacuum, ignore it
                Ok(None)
            } else {
                Err(e.into())
            };
        }
    };

    match gc_root {
        Ok((gc_root, _)) => {
            info!(
                "gc_root found: id={:?}, ts={:?}",
                gc_root.snapshot_id, gc_root.timestamp
            );
            let mut gc_candidates = Vec::with_capacity(snapshots_before_lvt.len());

            for snapshot in snapshots_before_lvt.iter() {
                let path = snapshot.path();
                let last_part = path.rsplit('/').next().unwrap();
                if last_part.starts_with(VACUUM2_OBJECT_KEY_PREFIX) {
                    gc_candidates.push(path.to_owned());
                } else {
                    // This snapshot is created by a node of the previous version which does not
                    // support vacuum2, we rely on the `ASSUMPTION_MAX_TXN_DURATION` to identify if
                    // it is available to be vacuumed.
                    let last_modified = match snapshot.metadata().last_modified() {
                        None => op.stat(path).await?.last_modified().ok_or_else(|| {
                            ErrorCode::StorageOther(format!(
                                "Failed to get `last_modified` metadata of the snapshot object '{}'",
                                path
                            ))
                        })?,
                        Some(v) => v,
                    };
                    if last_modified + ASSUMPTION_MAX_TXN_DURATION < gc_root_meta_ts {
                        gc_candidates.push(path.to_owned());
                    }
                }
            }

            let gc_root_idx = gc_candidates.binary_search(&gc_root_path).map_err(|_| {
                ErrorCode::Internal(format!(
                    "gc root path {} should be one of the candidates, candidates: {:?}",
                    gc_root_path, gc_candidates
                ))
            })?;
            let snapshots_to_gc = gc_candidates[..gc_root_idx].to_vec();
            Ok(Some(SnapshotGcSelection {
                gc_root,
                snapshots_to_gc,
                gc_root_meta_ts,
                gc_root_path,
            }))
        }
        Err(e) => {
            info!("read gc_root {} failed: {:?}", gc_root_path, e);
            Ok(None)
        }
    }
}

/// Try set lvt as min(latest_snapshot.timestamp, now - retention_time).
///
/// Return `None` means we stop vacuuming, but don't want to report error to user.
async fn set_lvt(
    latest_snapshot: Arc<TableSnapshot>,
    ctx: &dyn TableContext,
    retention_period: TimeDelta,
    table_id: u64,
) -> Result<Option<DateTime<Utc>>> {
    if !is_uuid_v7(&latest_snapshot.snapshot_id) {
        info!(
            "Latest snapshot is not v7, stopping vacuum: {:?}",
            latest_snapshot.snapshot_id
        );
        return Ok(None);
    }
    let catalog = ctx.get_default_catalog()?;
    // safe to unwrap, as we have checked the snapshot_id is UUID v7
    let latest_ts = latest_snapshot.timestamp.unwrap();
    let lvt_point_candidate = std::cmp::min(Utc::now() - retention_period, latest_ts);

    let lvt_point = catalog
        .set_table_lvt(
            &LeastVisibleTimeIdent::new(ctx.get_tenant(), table_id),
            &LeastVisibleTime::new(lvt_point_candidate),
        )
        .await?
        .time;
    Ok(Some(lvt_point))
}

fn slice_summary<T: std::fmt::Debug>(s: &[T]) -> String {
    if s.len() > 10 {
        let first_five = &s[..5];
        let last_five = &s[s.len() - 5..];
        format!(
            "First five: {:?}, Last five: {:?},Len: {}",
            first_five,
            last_five,
            s.len()
        )
    } else {
        format!("{:?}", s)
    }
}
