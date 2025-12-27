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
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::SegmentInfo;

const DRY_RUN_LIMIT: usize = 1000;

#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotReferencedFiles {
    pub segments: HashSet<String>,
    pub blocks: HashSet<String>,
    pub blocks_index: HashSet<String>,
    pub segments_stats: HashSet<String>,
}

impl SnapshotReferencedFiles {
    pub fn all_files(&self) -> Vec<String> {
        let mut files = vec![];
        for file in &self.segments {
            files.push(file.clone());
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

// return all the segment\block\index files referenced by current snapshot.
#[async_backtrace::framed]
pub async fn get_snapshot_referenced_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<Option<SnapshotReferencedFiles>> {
    // 1. Find all segments referenced by the current snapshots (including branches and tags)
    let segments_opt = fuse_table
        .get_snapshot_referenced_segments(ctx.clone(), |status| {
            ctx.set_status_info(&status);
        })
        .await?;

    let Some(segments) = segments_opt else {
        return Ok(None);
    };

    let segment_refs: Vec<&_> = segments.iter().collect();
    let locations_referenced = fuse_table
        .get_block_locations(ctx.clone(), &segment_refs, false, false)
        .await?;

    Ok(Some(SnapshotReferencedFiles {
        segments: segments.into_iter().map(|(location, _)| location).collect(),
        blocks: locations_referenced.block_location,
        blocks_index: locations_referenced.bloom_location,
        segments_stats: locations_referenced.hll_location,
    }))
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
pub async fn do_gc_orphan_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
) -> Result<()> {
    // 1. Get all the files referenced by the current snapshot
    let Some(referenced_files) = get_snapshot_referenced_files(fuse_table, ctx).await? else {
        return Ok(());
    };
    let status = format!(
        "gc orphan: read referenced files:{},{},{},{}, cost:{:?}",
        referenced_files.segments.len(),
        referenced_files.blocks.len(),
        referenced_files.blocks_index.len(),
        referenced_files.segments_stats.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 2. Purge orphan segment files.
    // 2.1 Get orphan segment files to be purged
    let location_gen = fuse_table.meta_location_generator();
    let segment_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_location_prefix(),
        referenced_files.segments,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan: read segment_locations_to_be_purged:{}, cost:{:?}, retention_time: {}",
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
        "gc orphan: purged segment files:{}, cost:{:?}",
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 3. Purge orphan block files.
    // 3.1 Get orphan block files to be purged
    let block_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_location_prefix(),
        referenced_files.blocks,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan: read block_locations_to_be_purged:{}, cost:{:?}",
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
        "gc orphan: purged block files:{}, cost:{:?}",
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 4. Purge orphan block index files.
    // 4.1 Get orphan block index files to be purged
    let index_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.block_bloom_index_prefix(),
        referenced_files.blocks_index,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan: read index_locations_to_be_purged:{}, cost:{:?}",
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
        "gc orphan: purged block index files:{}, cost:{:?}",
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);

    // 5. Purge orphan segment stats files.
    // 5.1 Get orphan segment stats files to be purged
    let stats_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_statistics_location_prefix(),
        referenced_files.segments_stats,
        retention_time,
    )
    .await?;
    let status = format!(
        "gc orphan: read stats_locations_to_be_purged:{}, cost:{:?}",
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
        "gc orphan: purged segment stats files:{}, cost:{:?}",
        purged_file_num,
        start.elapsed()
    );
    ctx.set_status_info(&status);
    Ok(())
}

#[async_backtrace::framed]
pub async fn do_dry_run_orphan_files(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    start: Instant,
    purge_files: &mut Vec<String>,
    dry_run_limit: usize,
) -> Result<()> {
    // 1. Get all the files referenced by the current snapshot
    let Some(referenced_files) = get_snapshot_referenced_files(fuse_table, ctx).await? else {
        return Ok(());
    };
    let status = format!(
        "dry_run orphan: read referenced files:{},{},{},{}, cost:{:?}",
        referenced_files.segments.len(),
        referenced_files.blocks.len(),
        referenced_files.blocks_index.len(),
        referenced_files.segments_stats.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    let location_gen = fuse_table.meta_location_generator();
    // 2. Get purge orphan segment files.
    let segment_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_location_prefix(),
        referenced_files.segments,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan: read segment_locations_to_be_purged:{}, cost:{:?}",
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
        referenced_files.blocks,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan: read block_locations_to_be_purged:{}, cost:{:?}",
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
        referenced_files.blocks_index,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan: read index_locations_to_be_purged:{}, cost:{:?}",
        index_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    purge_files.extend(index_locations_to_be_purged);

    // 5. Get purge orphan segment stats files.
    let stats_locations_to_be_purged = get_orphan_files_to_be_purged(
        fuse_table,
        location_gen.segment_statistics_location_prefix(),
        referenced_files.segments_stats,
        retention_time,
    )
    .await?;
    let status = format!(
        "dry_run orphan: read stats_locations_to_be_purged:{}, cost:{:?}",
        stats_locations_to_be_purged.len(),
        start.elapsed()
    );
    ctx.set_status_info(&status);

    purge_files.extend(stats_locations_to_be_purged);

    Ok(())
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
    let status = format!("do_vacuum: purged table, cost:{:?}", start.elapsed());
    ctx.set_status_info(&status);

    let catalog = ctx.get_default_catalog()?;
    let table_lvt = catalog
        .get_table_lvt(&LeastVisibleTimeIdent::new(
            ctx.get_tenant(),
            fuse_table.get_table_info().ident.table_id,
        ))
        .await?;
    let table = fuse_table.refresh(ctx.as_ref()).await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    // Technically, we should derive a reasonable retention period from the ByNumOfSnapshotsToKeep policy,
    // but it's not worth the effort since VACUUM2 will replace legacy purge and vacuum soon.
    // Use the table retention period for now.
    let retention_period = if fuse_table.is_transient() {
        // For transient table, keep no history data
        TimeDelta::zero()
    } else {
        fuse_table.get_data_retention_period(ctx.as_ref())?
    };
    let candidate_time = Utc::now() - retention_period;
    let retention_time = if let Some(lvt) = table_lvt {
        std::cmp::min(lvt.time, candidate_time)
    } else {
        candidate_time
    };
    if let Some(mut purge_files) = purge_files_opt {
        let dry_run_limit = dry_run_limit.unwrap();
        if purge_files.len() < dry_run_limit {
            do_dry_run_orphan_files(
                fuse_table,
                &ctx,
                retention_time,
                start,
                &mut purge_files,
                dry_run_limit,
            )
            .await?;
        }

        if purge_files.len() > dry_run_limit {
            purge_files = purge_files.into_iter().take(dry_run_limit).collect();
        }
        Ok(Some(purge_files))
    } else {
        debug_assert!(dry_run_limit.is_none());
        do_gc_orphan_files(fuse_table, &ctx, retention_time, start).await?;
        Ok(None)
    }
}
