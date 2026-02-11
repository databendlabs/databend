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
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::GcDroppedTableBranchReq;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::SnapshotGcSelection;
use databend_meta_types::MatchSeq;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::try_extract_uuid_str_from_path;
use log::info;

type OwnerPrefixes = Vec<(u64, String)>;
type ProtectedSegmentsByOwner = HashMap<u64, HashSet<Location>>;
type ProtectedBlocksByOwner = HashMap<u64, HashSet<String>>;
type RetainableBranch = (Box<FuseTable>, Option<OwnerSnapshotResult>);
type BeyondRetentionBranch = (String, Box<FuseTable>);

struct OwnerGcRoot {
    gc_root: Arc<TableSnapshot>,
    gc_root_loc: String,
    gc_root_meta_ts: DateTime<Utc>,
    protected_segments: HashSet<Location>,
    snapshots_to_gc: Vec<String>,
}

struct OwnerSnapshotResult {
    owner_table_id: u64,
    gc_root_snapshot_ts: DateTime<Utc>,
    gc_root_snapshot_meta_ts: DateTime<Utc>,
    protected_segments: HashSet<Location>,
    snapshot_files_to_gc: Vec<String>,
    needs_data_cleanup: bool,
}

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

    let fuse_table = FuseTable::try_from_table(table)?;
    if fuse_table.read_table_snapshot().await?.is_none() {
        info!("Table {} has no snapshot, stopping vacuum", table_info.desc);
        return Ok(vec![]);
    }

    let now = Utc::now();
    let retention_boundary =
        now - Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);

    // Step 1: vacuum base snapshots first.
    let base_owner_result =
        vacuum_base_snapshot_phase(fuse_table, &ctx, respect_flash_back).await?;

    let mut files_to_gc = base_owner_result.snapshot_files_to_gc.clone();
    let mut owner_prefixes: OwnerPrefixes = vec![(
        fuse_table.get_id(),
        format!("{}/", fuse_table.meta_location_generator().prefix()),
    )];

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

    // Step 2: classify branches using the refreshed base view and finish owner-local snapshot cleanup.
    let mut retainable_branches: Vec<RetainableBranch> = Vec::new();
    let mut beyond_retention_branches: Vec<BeyondRetentionBranch> = Vec::new();
    for branch in history_branches {
        let branch_name = branch.branch_name.clone();
        let branch_id = branch.branch_id.table_id;
        let branch_table = fuse_table.branch_table_from_meta(branch.clone(), &s3_storage_class)?;
        owner_prefixes.push((
            branch_id,
            format!("{}/", branch_table.meta_location_generator().prefix()),
        ));

        if branch
            .branch_meta
            .data
            .drop_on
            .is_some_and(|drop_on| drop_on < retention_boundary)
        {
            beyond_retention_branches.push((branch_name, branch_table));
            continue;
        }

        if branch.expire_at.is_some_and(|expire_at| expire_at <= now) {
            if let Err(err) = catalog
                .drop_table_branch(DropTableBranchReq {
                    tenant: ctx.get_tenant(),
                    table_id: fuse_table.get_id(),
                    branch_name: branch_name.clone(),
                    branch_id,
                    catalog_name: Some(fuse_table.get_table_info().catalog().to_string()),
                })
                .await
            {
                info!(
                    "drop expired branch failed, ignored, table: {}, branch: {}, err: {}",
                    fuse_table.get_table_info().desc,
                    branch_name,
                    err
                );
            }
        }

        let owner_result =
            vacuum_branch_snapshot_phase(branch_table.as_ref(), &ctx, respect_flash_back).await?;
        if let Some(owner_result) = owner_result.as_ref() {
            files_to_gc.extend(owner_result.snapshot_files_to_gc.iter().cloned());
        }
        retainable_branches.push((branch_table, owner_result));
    }

    // Step 3: merge all protected segment roots that must survive cross-owner cleanup.
    let mut protected_segments_by_owner: ProtectedSegmentsByOwner = HashMap::new();
    merge_protected_segments(
        &owner_prefixes,
        &base_owner_result,
        &mut protected_segments_by_owner,
    )?;
    for (_, owner_result) in &retainable_branches {
        if let Some(owner_result) = owner_result.as_ref() {
            merge_protected_segments(
                &owner_prefixes,
                owner_result,
                &mut protected_segments_by_owner,
            )?;
        }
    }

    // Step 4: expand protected blocks from protected segments.
    // Segment roots are the cross-owner protection boundary. Blocks are derived from them
    // so we only need to keep one global block protection map.
    let protected_blocks_by_owner = collect_protected_blocks_by_owner(
        &ctx,
        fuse_table,
        &owner_prefixes,
        &protected_segments_by_owner,
    )
    .await?;

    // Step 5: final-gc beyond-retention branches that no longer own protected data.
    let mut gc_pending_branches: Vec<BeyondRetentionBranch> = Vec::new();
    for (branch_name, branch_table) in beyond_retention_branches {
        let branch_id = branch_table.get_id();
        let (protected_segments, protected_blocks) = owner_protection(
            &protected_segments_by_owner,
            &protected_blocks_by_owner,
            branch_id,
        );

        if protected_segments.is_empty() && protected_blocks.is_empty() {
            files_to_gc.extend(
                final_gc_branch(
                    &ctx,
                    branch_table.as_ref(),
                    fuse_table.get_id(),
                    &branch_name,
                )
                .await?,
            );
        } else {
            gc_pending_branches.push((branch_name, branch_table));
        }
    }

    // Step 6: clean owner data files. Retainable owners use gc-root boundaries; gc-pending
    // branches scan their whole prefix and keep only globally protected data.
    if base_owner_result.needs_data_cleanup {
        let (protected_segments, protected_blocks) = owner_protection(
            &protected_segments_by_owner,
            &protected_blocks_by_owner,
            base_owner_result.owner_table_id,
        );
        files_to_gc.extend(
            cleanup_owner_data(
                fuse_table,
                ctx.clone(),
                Some((
                    base_owner_result.gc_root_snapshot_ts,
                    base_owner_result.gc_root_snapshot_meta_ts,
                )),
                vec![],
                protected_segments,
                protected_blocks,
            )
            .await?,
        );
    }

    for (branch_table, owner_result) in retainable_branches {
        let Some(owner_result) = owner_result else {
            continue;
        };
        if !owner_result.needs_data_cleanup {
            continue;
        }
        let (protected_segments, protected_blocks) = owner_protection(
            &protected_segments_by_owner,
            &protected_blocks_by_owner,
            owner_result.owner_table_id,
        );
        files_to_gc.extend(
            cleanup_owner_data(
                branch_table.as_ref(),
                ctx.clone(),
                Some((
                    owner_result.gc_root_snapshot_ts,
                    owner_result.gc_root_snapshot_meta_ts,
                )),
                vec![],
                protected_segments,
                protected_blocks,
            )
            .await?,
        );
    }

    for (_branch_name, branch_table) in gc_pending_branches {
        let branch_id = branch_table.get_id();
        let (protected_segments, protected_blocks) = owner_protection(
            &protected_segments_by_owner,
            &protected_blocks_by_owner,
            branch_id,
        );
        files_to_gc.extend(
            cleanup_owner_data(
                branch_table.as_ref(),
                ctx.clone(),
                None,
                branch_table
                    .list_files_for_gc(
                        branch_table
                            .meta_location_generator()
                            .snapshot_location_prefix(),
                        None,
                    )
                    .await?,
                protected_segments,
                protected_blocks,
            )
            .await?,
        );
    }

    Ok(files_to_gc)
}

#[async_backtrace::framed]
async fn vacuum_base_snapshot_phase(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<OwnerSnapshotResult> {
    let gc_root = select_owner_gc_root(fuse_table, ctx, respect_flash_back)
        .await?
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "base table {} should have snapshot for vacuum2",
                fuse_table.get_table_info().desc
            ))
        })?;
    let mut protected_segments = gc_root.protected_segments.clone();

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
                info!(
                    "drop expired tag failed, ignored, table: {}, err: {}",
                    fuse_table.get_table_info().desc,
                    err
                );
            }
            continue;
        }

        protected_snapshot_locs.insert(seq_tag.data.snapshot_loc.clone());
        if seq_tag.data.snapshot_loc < gc_root.gc_root_loc {
            if let Some(snapshot) = SnapshotsIO::read_snapshot_for_vacuum(
                fuse_table.get_operator(),
                &seq_tag.data.snapshot_loc,
            )
            .await?
            {
                protected_segments.extend(snapshot.segments.iter().cloned());
            }
        }
    }

    let mut snapshot_files_to_gc = gc_root.snapshots_to_gc.clone();
    snapshot_files_to_gc.retain(|path| !protected_snapshot_locs.contains(path));
    let owner_result = finalize_owner_snapshot_phase(
        fuse_table,
        ctx.clone(),
        &gc_root,
        protected_segments,
        snapshot_files_to_gc,
    )
    .await?;

    Ok(owner_result)
}

#[async_backtrace::framed]
async fn vacuum_branch_snapshot_phase(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Option<OwnerSnapshotResult>> {
    // Branch snapshot cleanup is owner-local. Unlike base, there are no branch-local tags to
    // re-read after LVT, so the gc-root result can be finalized directly.
    let Some(gc_root) = select_owner_gc_root(fuse_table, ctx, respect_flash_back).await? else {
        return Ok(None);
    };
    let protected_segments = gc_root.protected_segments.clone();
    let snapshot_files_to_gc = gc_root.snapshots_to_gc.clone();

    Ok(Some(
        finalize_owner_snapshot_phase(
            fuse_table,
            ctx.clone(),
            &gc_root,
            protected_segments,
            snapshot_files_to_gc,
        )
        .await?,
    ))
}

#[async_backtrace::framed]
async fn select_owner_gc_root(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Option<OwnerGcRoot>> {
    let Some(latest_snapshot) = fuse_table.read_table_snapshot().await? else {
        return Ok(None);
    };
    let latest_snapshot_loc = fuse_table.snapshot_loc().ok_or_else(|| {
        ErrorCode::Internal(format!(
            "table {} has snapshot but no snapshot location",
            fuse_table.get_table_info().desc
        ))
    })?;
    let selection =
        fuse_table.prepare_snapshot_gc_selection(ctx, latest_snapshot.clone(), respect_flash_back);
    let (gc_root, gc_root_loc, snapshots_to_gc, gc_root_meta_ts) = match selection.await? {
        Some(SnapshotGcSelection {
            gc_root,
            snapshots_to_gc,
            gc_root_meta_ts,
        }) => {
            let gc_root_loc = fuse_table
                .meta_location_generator()
                .gen_snapshot_location(&gc_root.snapshot_id, gc_root.format_version)?;
            (gc_root, gc_root_loc, snapshots_to_gc, gc_root_meta_ts)
        }
        None => {
            let gc_root_meta_ts = fuse_table
                .snapshot_last_modified(&latest_snapshot_loc)
                .await?;
            (
                latest_snapshot,
                latest_snapshot_loc.clone(),
                vec![],
                gc_root_meta_ts,
            )
        }
    };

    Ok(Some(OwnerGcRoot {
        // Data cleanup is owner-local, but protection still needs the full gc-root segment set so
        // later global block expansion can see cross-owner references.
        protected_segments: gc_root.segments.iter().cloned().collect(),
        gc_root,
        gc_root_loc,
        gc_root_meta_ts,
        snapshots_to_gc,
    }))
}

fn merge_protected_segments(
    owner_prefixes: &OwnerPrefixes,
    owner_result: &OwnerSnapshotResult,
    protected_segments_by_owner: &mut ProtectedSegmentsByOwner,
) -> Result<()> {
    for segment in &owner_result.protected_segments {
        let owner_id = owner_id_by_path(owner_prefixes, &segment.0)?;
        protected_segments_by_owner
            .entry(owner_id)
            .or_default()
            .insert(segment.clone());
    }
    Ok(())
}

#[async_backtrace::framed]
async fn collect_protected_blocks_by_owner(
    ctx: &Arc<dyn TableContext>,
    base_fuse_table: &FuseTable,
    owner_prefixes: &OwnerPrefixes,
    protected_segments_by_owner: &ProtectedSegmentsByOwner,
) -> Result<ProtectedBlocksByOwner> {
    let mut protected_blocks_by_owner: ProtectedBlocksByOwner = HashMap::new();

    for protected_segments in protected_segments_by_owner.values() {
        if protected_segments.is_empty() {
            continue;
        }

        let segment_refs = protected_segments.iter().collect::<Vec<_>>();
        let block_locations = base_fuse_table
            .get_block_locations(ctx.clone(), &segment_refs, false, false)
            .await?;
        for block_path in block_locations.block_location {
            let owner_id = owner_id_by_path(owner_prefixes, &block_path)?;
            protected_blocks_by_owner
                .entry(owner_id)
                .or_default()
                .insert(try_extract_uuid_str_from_path(&block_path)?.to_string());
        }
    }

    Ok(protected_blocks_by_owner)
}

#[async_backtrace::framed]
async fn cleanup_owner_data(
    table: &dyn Table,
    ctx: Arc<dyn TableContext>,
    cutoff: Option<(DateTime<Utc>, DateTime<Utc>)>,
    snapshots_to_gc: Vec<String>,
    protected_segments: HashSet<Location>,
    protected_blocks: HashSet<String>,
) -> Result<Vec<String>> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let table_info = fuse_table.get_table_info();

    let segments_to_gc = fuse_table
        .list_files_for_gc(
            fuse_table
                .meta_location_generator()
                .segment_location_prefix(),
            cutoff,
        )
        .await?
        .into_iter()
        .filter(|path| {
            !protected_segments.contains(&(
                path.clone(),
                TableMetaLocationGenerator::snapshot_version(path),
            ))
        })
        .collect::<Vec<_>>();
    let blocks_to_gc = filter_blocks_to_gc(
        fuse_table
            .list_files_for_gc(
                fuse_table.meta_location_generator().block_location_prefix(),
                cutoff,
            )
            .await?,
        &protected_blocks,
    )?;

    let files_to_gc = cleanup_owner_files(
        fuse_table,
        ctx,
        snapshots_to_gc,
        segments_to_gc,
        blocks_to_gc,
    )
    .await?;
    info!(
        "vacuum2 owner cleaned, table: {}, files: {}",
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
) -> Result<Vec<String>> {
    let table_info = fuse_table.get_table_info();
    let branch_dir = format!(
        "{}/",
        FuseTable::parse_storage_prefix_from_table_info(table_info)?
    );

    let catalog = ctx.get_catalog(table_info.catalog()).await?;
    if let Err(err) = catalog
        .gc_drop_table_branch(GcDroppedTableBranchReq {
            tenant: ctx.get_tenant(),
            table_id: base_table_id,
            branch_name: branch_name.to_string(),
            branch_id: fuse_table.get_id(),
        })
        .await
    {
        info!(
            "gc non-retainable branch kvs failed, ignored, table: {}, branch: {}, err: {}",
            table_info.desc, branch_name, err
        );
    }
    if let Err(err) = fuse_table.get_operator().remove_all(&branch_dir).await {
        info!(
            "cleanup non-retainable branch data failed, ignored, table: {}, branch: {}, err: {}",
            table_info.desc, branch_name, err
        );
    }
    Ok(vec![branch_dir])
}

async fn finalize_owner_snapshot_phase(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    gc_root: &OwnerGcRoot,
    protected_segments: HashSet<Location>,
    snapshot_files_to_gc: Vec<String>,
) -> Result<OwnerSnapshotResult> {
    fuse_table
        .cleanup_snapshot_files(&ctx, &snapshot_files_to_gc, false)
        .await?;
    let gc_root_snapshot_ts = gc_root.gc_root.timestamp.ok_or_else(|| {
        ErrorCode::IllegalReference(format!(
            "Table {} snapshot lacks required timestamp",
            fuse_table.get_table_info().desc
        ))
    })?;

    Ok(OwnerSnapshotResult {
        owner_table_id: fuse_table.get_id(),
        gc_root_snapshot_ts,
        gc_root_snapshot_meta_ts: gc_root.gc_root_meta_ts,
        protected_segments,
        needs_data_cleanup: !snapshot_files_to_gc.is_empty(),
        snapshot_files_to_gc,
    })
}

#[async_backtrace::framed]
async fn cleanup_owner_files(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    snapshots_to_gc: Vec<String>,
    segments_to_gc: Vec<String>,
    blocks_to_gc: Vec<String>,
) -> Result<Vec<String>> {
    // Companion files are derived from blocks/segments, so deleting them first keeps the data-file
    // deletion order aligned with the rest of vacuum2.
    let stats_to_gc = segments_to_gc
        .iter()
        .map(|path| {
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(path)
        })
        .collect::<Vec<_>>();
    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &fuse_table.get_table_info().meta.indexes;
    let mut indexes_to_gc = Vec::with_capacity(
        blocks_to_gc.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 3),
    );
    for loc in &blocks_to_gc {
        for index_id in &table_agg_index_ids {
            indexes_to_gc.push(
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                ),
            );
        }
        for idx in inverted_indexes.values() {
            indexes_to_gc.push(
                TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                    loc,
                    idx.name.as_str(),
                    idx.version.as_str(),
                ),
            );
        }
        indexes_to_gc
            .push(TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc));
        indexes_to_gc.push(TableMetaLocationGenerator::gen_virtual_block_location(loc));
    }
    let subject_files_to_gc = segments_to_gc
        .into_iter()
        .chain(blocks_to_gc)
        .chain(stats_to_gc)
        .collect::<Vec<_>>();

    let op = Files::create(ctx.clone(), fuse_table.get_operator());
    op.remove_file_in_batch(&indexes_to_gc).await?;
    op.remove_file_in_batch(&subject_files_to_gc).await?;
    fuse_table
        .cleanup_snapshot_files(&ctx, &snapshots_to_gc, false)
        .await?;

    Ok(subject_files_to_gc
        .into_iter()
        .chain(snapshots_to_gc)
        .chain(indexes_to_gc)
        .collect())
}

fn owner_protection(
    protected_segments_by_owner: &ProtectedSegmentsByOwner,
    protected_blocks_by_owner: &ProtectedBlocksByOwner,
    owner_id: u64,
) -> (HashSet<Location>, HashSet<String>) {
    (
        protected_segments_by_owner
            .get(&owner_id)
            .cloned()
            .unwrap_or_default(),
        protected_blocks_by_owner
            .get(&owner_id)
            .cloned()
            .unwrap_or_default(),
    )
}

fn owner_id_by_path(owner_prefixes: &OwnerPrefixes, path: &str) -> Result<u64> {
    owner_prefixes
        .iter()
        .find(|(_, prefix)| path.starts_with(prefix))
        .map(|(owner_table_id, _)| *owner_table_id)
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "cannot classify owner by path '{}', known prefixes: {:?}",
                path,
                owner_prefixes
                    .iter()
                    .map(|(_, prefix)| prefix.as_str())
                    .collect::<Vec<_>>()
            ))
        })
}

fn filter_blocks_to_gc(
    blocks: Vec<String>,
    protected_blocks: &HashSet<String>,
) -> Result<Vec<String>> {
    let mut blocks_to_gc = Vec::new();
    for path in blocks {
        let block_uuid = try_extract_uuid_str_from_path(&path)?.to_string();
        if !protected_blocks.contains(&block_uuid) {
            blocks_to_gc.push(path);
        }
    }
    Ok(blocks_to_gc)
}
