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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SnapshotLiteExtended;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use log::info;

use crate::storages::fuse::operations::common::collect_retainable_history_branch_tables;
use crate::storages::fuse::operations::common::split_locations_by_prefix;
use crate::storages::fuse::operations::common::split_segments_by_prefix;

const DRY_RUN_LIMIT: usize = 1000;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SnapshotReferencedFiles {
    pub segments: HashSet<Location>,
    pub blocks: HashSet<String>,
    pub blocks_index: HashSet<String>,
    pub segments_stats: HashSet<String>,
}

impl SnapshotReferencedFiles {
    pub fn merge(&mut self, other: SnapshotReferencedFiles) {
        self.segments.extend(other.segments);
        self.blocks.extend(other.blocks);
        self.blocks_index.extend(other.blocks_index);
        self.segments_stats.extend(other.segments_stats);
    }

    pub fn segment_paths(&self) -> HashSet<String> {
        self.segments
            .iter()
            .map(|(location, _)| location.clone())
            .collect()
    }

    pub fn all_files(&self) -> Vec<String> {
        let mut files = vec![];
        for file in &self.segments {
            files.push(file.0.clone());
        }
        for file in &self.blocks {
            files.push(file.clone());
        }
        for file in &self.blocks_index {
            files.push(file.clone());
        }
        for file in &self.segments_stats {
            files.push(file.clone());
        }
        files
    }
}

#[async_backtrace::framed]
async fn collect_table_snapshot_segments_without_refs(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Option<HashSet<Location>>> {
    let Some(root_snapshot_location) = fuse_table.snapshot_loc() else {
        return Ok(None);
    };

    let Some(root_snapshot) = SnapshotsIO::read_snapshot_for_vacuum(
        fuse_table.get_operator(),
        root_snapshot_location.as_str(),
    )
    .await?
    else {
        return Ok(None);
    };

    let root_snapshot_lite = Arc::new(SnapshotLiteExtended {
        format_version: TableMetaLocationGenerator::snapshot_version(
            root_snapshot_location.as_str(),
        ),
        snapshot_id: root_snapshot.snapshot_id,
        timestamp: root_snapshot.timestamp,
        segments: HashSet::from_iter(root_snapshot.segments.clone()),
        table_statistics_location: root_snapshot.table_statistics_location(),
    });

    let mut segments = root_snapshot_lite.segments.clone();
    let Some(snapshot_prefix) =
        SnapshotsIO::get_s3_prefix_from_file(root_snapshot_location.as_str())
    else {
        return Ok(Some(segments));
    };

    let snapshot_files =
        SnapshotsIO::list_files(fuse_table.get_operator(), snapshot_prefix.as_str(), None).await?;
    if snapshot_files.is_empty() {
        return Ok(Some(segments));
    }

    let snapshots_io = SnapshotsIO::create(ctx.clone(), fuse_table.get_operator());
    let chunk_size = ctx.get_settings().get_max_threads()? as usize;
    let chunk_size = chunk_size.max(1);
    for chunk in snapshot_files.chunks(chunk_size) {
        let snapshot_lite_extends = snapshots_io
            .read_snapshot_lite_extends(chunk, root_snapshot_lite.clone(), true)
            .await?;
        for snapshot_lite_extend in snapshot_lite_extends.into_iter().flatten() {
            segments.extend(snapshot_lite_extend.segments.into_iter());
        }
    }

    Ok(Some(segments))
}

#[async_backtrace::framed]
async fn collect_base_snapshot_referenced_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Option<SnapshotReferencedFiles>> {
    let Some(mut segments) = collect_table_snapshot_segments_without_refs(fuse_table, ctx).await?
    else {
        return Ok(None);
    };

    // Protect tags on base table as well.
    let table_id = fuse_table.get_id();
    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let tags = catalog
        .list_table_tags(ListTableTagsReq {
            table_id,
            include_expired: false,
        })
        .await?;

    let base_segment_prefix = fuse_table
        .meta_location_generator()
        .segment_location_prefix();
    for (_tag_name, seq_tag) in tags {
        if let Some(snapshot) = SnapshotsIO::read_snapshot_for_vacuum(
            fuse_table.get_operator(),
            &seq_tag.data.snapshot_loc,
        )
        .await?
        {
            segments.extend(
                snapshot
                    .segments
                    .iter()
                    .filter(|(location, _)| location.starts_with(base_segment_prefix))
                    .cloned(),
            );
        }
    }

    let segment_refs: Vec<&Location> = segments.iter().collect();
    let locations_referenced = fuse_table
        .get_block_locations(ctx.clone(), &segment_refs, false, false)
        .await?;
    let location_gen = fuse_table.meta_location_generator();

    Ok(Some(SnapshotReferencedFiles {
        segments,
        blocks: locations_referenced
            .block_location
            .into_iter()
            .filter(|location| location.starts_with(location_gen.block_location_prefix()))
            .collect(),
        blocks_index: locations_referenced
            .bloom_location
            .into_iter()
            .filter(|location| location.starts_with(location_gen.block_bloom_index_prefix()))
            .collect(),
        segments_stats: locations_referenced
            .hll_location
            .into_iter()
            .filter(|location| {
                location.starts_with(location_gen.segment_statistics_location_prefix())
            })
            .collect(),
    }))
}

#[async_backtrace::framed]
async fn collect_branch_and_base_referenced_files(
    branch_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    base_segment_prefix: &str,
    base_block_prefix: &str,
    base_block_index_prefix: &str,
    base_segment_stats_prefix: &str,
) -> Result<Option<(SnapshotReferencedFiles, SnapshotReferencedFiles)>> {
    let segments_opt = branch_table
        .collect_table_snapshot_segments(
            ctx.clone(),
            ctx.get_settings().get_max_threads()? as usize,
            &|status| {
                ctx.set_status_info(&status);
            },
        )
        .await?;

    let Some(segments) = segments_opt else {
        return Ok(None);
    };

    let branch_segment_prefix = branch_table
        .meta_location_generator()
        .segment_location_prefix();
    let (branch_segments, base_segments) =
        split_segments_by_prefix(segments, branch_segment_prefix, base_segment_prefix);

    let mut branch_refs = SnapshotReferencedFiles {
        segments: branch_segments,
        blocks: HashSet::new(),
        blocks_index: HashSet::new(),
        segments_stats: HashSet::new(),
    };
    let mut base_refs = SnapshotReferencedFiles {
        segments: base_segments,
        blocks: HashSet::new(),
        blocks_index: HashSet::new(),
        segments_stats: HashSet::new(),
    };

    let branch_segment_refs: Vec<&Location> = branch_refs.segments.iter().collect();
    if branch_segment_refs.is_empty() {
        return Ok(Some((branch_refs, base_refs)));
    }

    let location_refs = branch_table
        .get_block_locations(ctx.clone(), &branch_segment_refs, false, false)
        .await?;
    let location_gen = branch_table.meta_location_generator();

    let (branch_blocks, base_blocks) = split_locations_by_prefix(
        location_refs.block_location,
        location_gen.block_location_prefix(),
        base_block_prefix,
    );
    let (branch_block_indexes, base_block_indexes) = split_locations_by_prefix(
        location_refs.bloom_location,
        location_gen.block_bloom_index_prefix(),
        base_block_index_prefix,
    );
    let (branch_segment_stats, base_segment_stats) = split_locations_by_prefix(
        location_refs.hll_location,
        location_gen.segment_statistics_location_prefix(),
        base_segment_stats_prefix,
    );

    branch_refs.blocks = branch_blocks;
    branch_refs.blocks_index = branch_block_indexes;
    branch_refs.segments_stats = branch_segment_stats;

    base_refs.blocks = base_blocks;
    base_refs.blocks_index = base_block_indexes;
    base_refs.segments_stats = base_segment_stats;

    Ok(Some((branch_refs, base_refs)))
}

#[async_backtrace::framed]
async fn merge_extra_segment_references_into_base_blocks(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    extra_segments: &HashSet<Location>,
    base_refs: &mut SnapshotReferencedFiles,
) -> Result<()> {
    if extra_segments.is_empty() {
        return Ok(());
    }

    let segment_refs: Vec<&Location> = extra_segments.iter().collect();
    let extra_locations = fuse_table
        .get_block_locations(ctx.clone(), &segment_refs, false, false)
        .await?;
    let location_gen = fuse_table.meta_location_generator();

    base_refs.blocks.extend(
        extra_locations
            .block_location
            .into_iter()
            .filter(|location| location.starts_with(location_gen.block_location_prefix())),
    );
    base_refs.blocks_index.extend(
        extra_locations
            .bloom_location
            .into_iter()
            .filter(|location| location.starts_with(location_gen.block_bloom_index_prefix())),
    );
    base_refs
        .segments_stats
        .extend(extra_locations.hll_location.into_iter().filter(|location| {
            location.starts_with(location_gen.segment_statistics_location_prefix())
        }));

    Ok(())
}

// return orphan files to be purged
#[async_backtrace::framed]
async fn get_orphan_files_to_be_purged(
    fuse_table: &FuseTable,
    prefix: &str,
    referenced_files: HashSet<String>,
    retention_time: DateTime<Utc>,
) -> Result<Vec<String>> {
    let prefix = prefix.to_string();
    fuse_table
        .list_files(prefix, |location, modified| {
            modified <= retention_time && !referenced_files.contains(&location)
        })
        .await
}

#[async_backtrace::framed]
async fn do_gc_orphan_files_with_referenced_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
    namespace: &str,
    referenced_files: SnapshotReferencedFiles,
) -> Result<()> {
    let segment_num = referenced_files.segments.len();
    let block_num = referenced_files.blocks.len();
    let block_index_num = referenced_files.blocks_index.len();
    let segment_stats_num = referenced_files.segments_stats.len();
    let status = format!(
        "gc orphan [{}]: read referenced files:{},{},{},{}, cost:{:?}",
        namespace,
        segment_num,
        block_num,
        block_index_num,
        segment_stats_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    let SnapshotReferencedFiles {
        segments,
        blocks,
        blocks_index,
        segments_stats,
    } = referenced_files;

    // 2. Purge orphan segment files.
    // 2.1 Get orphan segment files to be purged
    let location_gen = fuse_table.meta_location_generator();
    let segment_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_location_prefix(),
        segments.into_iter().map(|(location, _)| location).collect(),
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan [{}]: read segment_locations_to_be_purged:{}, cost:{:?}, retention_time: {}",
        namespace,
        segment_locations_to_be_purged.len(),
        start.elapsed(),
        retention_time
    );
    ctx.set_status_info(&status);

    // 2.2 Delete all the orphan segment files to be purged
    let purged_file_num = segment_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files_and_cache::<SegmentInfo, _>(
            ctx.clone(),
            HashSet::from_iter(segment_locations_to_be_purged.into_iter()),
        )
        .await?;

    let status = format!(
        "gc orphan [{}]: purged segment files:{}, cost:{:?}",
        namespace,
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 3. Purge orphan block files.
    // 3.1 Get orphan block files to be purged
    let block_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_location_prefix(),
        blocks,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan [{}]: read block_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        block_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 3.2 Delete all the orphan block files to be purged
    let purged_file_num = block_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files(
            ctx.clone(),
            HashSet::from_iter(block_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan [{}]: purged block files:{}, cost:{:?}",
        namespace,
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 4. Purge orphan block index files.
    // 4.1 Get orphan block index files to be purged
    let index_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_bloom_index_prefix(),
        blocks_index,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan [{}]: read index_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        index_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 4.2 Delete all the orphan block index files to be purged
    let purged_file_num = index_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files(
            ctx.clone(),
            HashSet::from_iter(index_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan [{}]: purged block index files:{}, cost:{:?}",
        namespace,
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 5. Purge orphan segment stats files.
    // 5.1 Get orphan segment stats files to be purged
    let stats_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_statistics_location_prefix(),
        segments_stats,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan [{}]: read stats_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        stats_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 5.2 Delete all the orphan segment stats files to be purged
    let purged_file_num = stats_locations_to_be_purged.len();
    fuse_table
        .try_purge_location_files(
            ctx.clone(),
            HashSet::from_iter(stats_locations_to_be_purged.into_iter()),
        )
        .await?;
    let status = format!(
        "gc orphan [{}]: purged segment stats files:{}, cost:{:?}",
        namespace,
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);
    Ok(())
}

#[async_backtrace::framed]
async fn do_dry_run_orphan_files_with_referenced_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
    purge_files: &mut Vec<String>,
    dry_run_limit: usize,
    namespace: &str,
    referenced_files: SnapshotReferencedFiles,
) -> Result<()> {
    let segment_num = referenced_files.segments.len();
    let block_num = referenced_files.blocks.len();
    let block_index_num = referenced_files.blocks_index.len();
    let segment_stats_num = referenced_files.segments_stats.len();
    let status = format!(
        "dry_run orphan [{}]: read referenced files:{},{},{},{}, cost:{:?}",
        namespace,
        segment_num,
        block_num,
        block_index_num,
        segment_stats_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    let SnapshotReferencedFiles {
        segments,
        blocks,
        blocks_index,
        segments_stats,
    } = referenced_files;

    let location_gen = fuse_table.meta_location_generator();
    // 2. Get purge orphan segment files.
    let segment_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_location_prefix(),
        segments.into_iter().map(|(location, _)| location).collect(),
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan [{}]: read segment_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        segment_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    purge_files.extend(segment_locations_to_be_purged);
    if purge_files.len() >= dry_run_limit {
        return Ok(());
    }

    // 3. Get purge orphan block files.
    let block_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_location_prefix(),
        blocks,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan [{}]: read block_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        block_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);
    purge_files.extend(block_locations_to_be_purged);
    if purge_files.len() >= dry_run_limit {
        return Ok(());
    }

    // 4. Get purge orphan block index files.
    let index_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_bloom_index_prefix(),
        blocks_index,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan [{}]: read index_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        index_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    purge_files.extend(index_locations_to_be_purged);

    // 5. Get purge orphan segment stats files.
    let stats_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_statistics_location_prefix(),
        segments_stats,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan [{}]: read stats_locations_to_be_purged:{}, cost:{:?}",
        namespace,
        stats_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    purge_files.extend(stats_locations_to_be_purged);

    Ok(())
}

#[async_backtrace::framed]
async fn calc_orphan_retention_time(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let catalog = ctx.get_default_catalog()?;
    let table_lvt = catalog
        .get_table_lvt(&LeastVisibleTimeIdent::new(
            ctx.get_tenant(),
            fuse_table.get_table_info().ident.table_id,
        ))
        .await?;

    let retention_period = if fuse_table.is_transient() {
        TimeDelta::zero()
    } else {
        fuse_table.get_data_retention_period(ctx.as_ref())?
    };
    let candidate_time = now - retention_period;
    Ok(if let Some(lvt) = table_lvt {
        std::cmp::min(lvt.time, candidate_time)
    } else {
        candidate_time
    })
}

#[async_backtrace::framed]
pub async fn do_vacuum(
    table: &dyn Table,
    ctx: Arc<dyn TableContext>,
    dry_run: bool,
) -> Result<Option<Vec<String>>> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let start = Instant::now();
    // First, do purge
    let dry_run_limit = if dry_run { Some(DRY_RUN_LIMIT) } else { None };
    // Let the table navigate to the point according to the table's retention policy.
    let navigation_point = None;
    let purge_files_opt = fuse_table
        .purge(ctx.clone(), navigation_point, dry_run_limit, dry_run)
        .await?;
    ctx.set_status_info(&format!(
        "do_vacuum: purged table, cost:{:?}",
        start.elapsed()
    ));

    let table = fuse_table.refresh(ctx.as_ref()).await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let retain_branches = collect_retainable_history_branch_tables(fuse_table, &ctx).await?;
    let branch_num = retain_branches.len();
    ctx.set_status_info(&format!(
        "do_vacuum: collected retainable history branches for orphan gc, branches:{}, cost:{:?}",
        branch_num,
        start.elapsed()
    ));

    let now = Utc::now();
    let location_gen = fuse_table.meta_location_generator();
    let base_segment_prefix = location_gen.segment_location_prefix().to_string();
    let base_block_prefix = location_gen.block_location_prefix().to_string();
    let base_block_index_prefix = location_gen.block_bloom_index_prefix().to_string();
    let base_segment_stats_prefix = location_gen
        .segment_statistics_location_prefix()
        .to_string();
    let s3_storage_class = ctx.get_settings().get_s3_storage_class()?;

    if let Some(mut purge_files) = purge_files_opt {
        let dry_run_limit = dry_run_limit.unwrap();
        if purge_files.len() < dry_run_limit {
            let mut base_refs_from_branches = SnapshotReferencedFiles::default();

            for branch in retain_branches {
                if purge_files.len() >= dry_run_limit {
                    break;
                }

                let branch_name = branch.branch_name.clone();
                let branch_id = branch.branch_id.table_id;
                let branch_table = fuse_table.branch_table_from_meta(branch, &s3_storage_class)?;

                let Some((branch_refs, base_refs)) = collect_branch_and_base_referenced_files(
                    &branch_table,
                    &ctx,
                    base_segment_prefix.as_str(),
                    base_block_prefix.as_str(),
                    base_block_index_prefix.as_str(),
                    base_segment_stats_prefix.as_str(),
                )
                .await?
                else {
                    info!(
                        "do_vacuum: skip branch without snapshot refs, table:{}, branch:{}, branch_id:{}",
                        fuse_table.get_table_info().desc,
                        branch_name,
                        branch_id,
                    );
                    continue;
                };
                let branch_ref_file_num = branch_refs.all_files().len();
                let base_ref_file_num = base_refs.all_files().len();
                let retention_time = calc_orphan_retention_time(&branch_table, &ctx, now).await?;
                do_dry_run_orphan_files_with_referenced_files(
                    &branch_table,
                    &ctx,
                    retention_time,
                    start,
                    &mut purge_files,
                    dry_run_limit,
                    branch_table.get_table_info().desc.as_str(),
                    branch_refs,
                )
                .await?;
                ctx.set_status_info(&format!(
                    "do_vacuum: dry-run branch done, table:{}, branch:{}, branch_files:{}, base_ref_files:{}",
                    fuse_table.get_table_info().desc,
                    branch_name,
                    branch_ref_file_num,
                    base_ref_file_num
                ));
                base_refs_from_branches.merge(base_refs);
            }

            if purge_files.len() < dry_run_limit {
                if let Some(mut base_refs) =
                    collect_base_snapshot_referenced_files(fuse_table, &ctx).await?
                {
                    let extra_segments: HashSet<Location> = base_refs_from_branches
                        .segments
                        .difference(&base_refs.segments)
                        .cloned()
                        .collect();
                    merge_extra_segment_references_into_base_blocks(
                        fuse_table,
                        &ctx,
                        &extra_segments,
                        &mut base_refs_from_branches,
                    )
                    .await?;
                    base_refs.merge(base_refs_from_branches);

                    let retention_time = calc_orphan_retention_time(fuse_table, &ctx, now).await?;
                    do_dry_run_orphan_files_with_referenced_files(
                        fuse_table,
                        &ctx,
                        retention_time,
                        start,
                        &mut purge_files,
                        dry_run_limit,
                        fuse_table.get_table_info().desc.as_str(),
                        base_refs,
                    )
                    .await?;
                }
            }
        }

        if purge_files.len() > dry_run_limit {
            purge_files = purge_files.into_iter().take(dry_run_limit).collect();
        }
        Ok(Some(purge_files))
    } else {
        debug_assert!(dry_run_limit.is_none());
        let mut base_refs_from_branches = SnapshotReferencedFiles::default();

        for branch in retain_branches {
            let branch_name = branch.branch_name.clone();
            let branch_id = branch.branch_id.table_id;
            let branch_table = fuse_table.branch_table_from_meta(branch, &s3_storage_class)?;
            let retention_time = calc_orphan_retention_time(&branch_table, &ctx, now).await?;
            let Some((branch_refs, base_refs)) = collect_branch_and_base_referenced_files(
                &branch_table,
                &ctx,
                base_segment_prefix.as_str(),
                base_block_prefix.as_str(),
                base_block_index_prefix.as_str(),
                base_segment_stats_prefix.as_str(),
            )
            .await?
            else {
                info!(
                    "do_vacuum: skip branch without snapshot refs, table:{}, branch:{}, branch_id:{}",
                    fuse_table.get_table_info().desc,
                    branch_name,
                    branch_id
                );
                continue;
            };
            let branch_ref_file_num = branch_refs.all_files().len();
            let base_ref_file_num = base_refs.all_files().len();
            do_gc_orphan_files_with_referenced_files(
                &branch_table,
                &ctx,
                retention_time,
                start,
                branch_table.get_table_info().desc.as_str(),
                branch_refs,
            )
            .await?;
            ctx.set_status_info(&format!(
                "do_vacuum: gc branch done, table:{}, branch:{}, branch_files:{}, base_ref_files:{}",
                fuse_table.get_table_info().desc,
                branch_name,
                branch_ref_file_num,
                base_ref_file_num
            ));
            base_refs_from_branches.merge(base_refs);
        }

        if let Some(mut base_refs) =
            collect_base_snapshot_referenced_files(fuse_table, &ctx).await?
        {
            let extra_segments: HashSet<Location> = base_refs_from_branches
                .segments
                .difference(&base_refs.segments)
                .cloned()
                .collect();
            merge_extra_segment_references_into_base_blocks(
                fuse_table,
                &ctx,
                &extra_segments,
                &mut base_refs_from_branches,
            )
            .await?;
            base_refs.merge(base_refs_from_branches);

            let retention_time = calc_orphan_retention_time(fuse_table, &ctx, now).await?;
            do_gc_orphan_files_with_referenced_files(
                fuse_table,
                &ctx,
                retention_time,
                start,
                fuse_table.get_table_info().desc.as_str(),
                base_refs,
            )
            .await?;
        }
        Ok(None)
    }
}
