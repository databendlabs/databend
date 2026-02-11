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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::SnapshotGcSelection;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use log::info;

#[async_backtrace::framed]
pub async fn do_vacuum2(
    table: &dyn Table,
    ctx: Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Vec<String>> {
    let table_info = table.get_table_info();
    {
        if ctx.txn_mgr().lock().is_active() {
            info!(
                "Transaction is active, skipping vacuum, target table {}",
                table_info.desc
            );
            return Ok(vec![]);
        }
    }

    let fuse_table = FuseTable::try_from_table(table)?;

    let Some(latest_snapshot) = fuse_table.read_table_snapshot().await? else {
        info!("Table {} has no snapshot, stopping vacuum", table_info.desc);
        return Ok(vec![]);
    };

    // Process snapshot refs (branches and tags) before main vacuum.
    let (mut ref_info, refs_before_lvt) = fuse_table
        .process_snapshot_refs_for_vacuum(&ctx, false, None)
        .await?;

    let Some(snapshot_gc_selection) = fuse_table
        .prepare_snapshot_gc_selection(&ctx, latest_snapshot, respect_flash_back)
        .await?
    else {
        return Ok(vec![]);
    };

    let SnapshotGcSelection {
        gc_root,
        mut snapshots_to_gc,
        gc_root_meta_ts,
    } = snapshot_gc_selection;

    let start = std::time::Instant::now();

    // After set_lvt, check for newly created refs to protect their snapshots
    // This handles the race condition where a branch is created between initial scan and set_lvt
    let (new_refs_info, _refs_after_lvt) = fuse_table
        .process_snapshot_refs_for_vacuum(&ctx, true, Some(&refs_before_lvt))
        .await?;
    if !new_refs_info.is_empty() {
        ref_info.merge(new_refs_info);
    }
    if !ref_info.protected_snapshots.is_empty() {
        snapshots_to_gc.retain(|path| !ref_info.protected_snapshots.contains(path));
    }

    let gc_root_timestamp = gc_root.timestamp.unwrap();
    ctx.set_status_info(&format!(
        "Selected gc_root for table {}, elapsed: {:?}, gc_root: {:?}, snapshots_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        gc_root,
        slice_summary(&snapshots_to_gc)
    ));

    let mut gc_root_segments: HashSet<_> = gc_root
        .segments
        .iter()
        .map(|(path, _)| path.clone())
        .collect();
    gc_root_segments.extend(
        ref_info
            .protected_segments
            .iter()
            .map(|(path, _)| path.clone()),
    );
    let segments_before_gc_root = fuse_table
        .list_files_until_timestamp(
            fuse_table
                .meta_location_generator()
                .segment_location_prefix(),
            gc_root_timestamp,
            false,
            Some(gc_root_meta_ts),
        )
        .await?
        .into_iter()
        .map(|v| v.path().to_owned())
        .collect::<Vec<_>>();

    ctx.set_status_info(&format!(
        "Listed segments before gc_root for table {}, elapsed: {:?}, segment_dir: {:?}, gc_root_timestamp: {:?}, segments: {:?}",
        table_info.desc,
        start.elapsed(),
        fuse_table.meta_location_generator().segment_location_prefix(),
        gc_root_timestamp,
        slice_summary(&segments_before_gc_root)
    ));

    let start = std::time::Instant::now();
    let segments_to_gc: Vec<String> = segments_before_gc_root
        .into_iter()
        .filter(|s| !gc_root_segments.contains(s))
        .collect();
    let stats_to_gc = segments_to_gc
        .iter()
        .map(|v| TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(v))
        .collect::<Vec<_>>();
    ctx.set_status_info(&format!(
        "Filtered segments_to_gc for table {}, elapsed: {:?}, segments_to_gc: {:?}, stats_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&segments_to_gc),
        slice_summary(&stats_to_gc)
    ));

    let start = std::time::Instant::now();
    let segments_io =
        SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());

    // Collect blocks from main gc_root
    let segments = segments_io
        .read_segments::<Arc<CompactSegmentInfo>>(&gc_root.segments, false)
        .await?;
    let mut gc_root_blocks = HashSet::new();
    for segment in segments {
        gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
    }

    let base_segment_prefix = fuse_table
        .meta_location_generator()
        .segment_location_prefix();
    let protected_base_segments = ref_info
        .protected_segments
        .iter()
        .filter(|(path, _)| path.starts_with(base_segment_prefix))
        .cloned()
        .collect::<Vec<_>>();
    if !protected_base_segments.is_empty() {
        let ref_segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(&protected_base_segments, false)
            .await?;
        for segment in ref_segments {
            gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
        }
    }
    gc_root_blocks.extend(ref_info.protected_blocks.iter().cloned());

    ctx.set_status_info(&format!(
        "Read segments for table {}, elapsed: {:?}, total protected blocks: {}",
        table_info.desc,
        start.elapsed(),
        gc_root_blocks.len()
    ));

    let start = std::time::Instant::now();
    let blocks_before_gc_root = fuse_table
        .list_files_until_timestamp(
            fuse_table.meta_location_generator().block_location_prefix(),
            gc_root_timestamp,
            false,
            Some(gc_root_meta_ts),
        )
        .await?
        .into_iter()
        .map(|v| v.path().to_owned())
        .collect::<Vec<_>>();

    ctx.set_status_info(&format!(
        "Listed blocks before gc_root for table {}, elapsed: {:?}, block_dir: {:?}, gc_root_timestamp: {:?}, blocks: {:?}",
        table_info.desc,
        start.elapsed(),
        fuse_table.meta_location_generator().block_location_prefix(),
        gc_root_timestamp,
        slice_summary(&blocks_before_gc_root)
    ));

    let start = std::time::Instant::now();
    let blocks_to_gc: Vec<String> = blocks_before_gc_root
        .into_iter()
        .filter(|b| !gc_root_blocks.contains(b))
        .collect();
    ctx.set_status_info(&format!(
        "Filtered blocks_to_gc for table {}, elapsed: {:?}, blocks_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&blocks_to_gc)
    ));

    let start = std::time::Instant::now();
    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &table_info.meta.indexes;
    let mut indexes_to_gc = Vec::with_capacity(
        blocks_to_gc.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 1),
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
    }

    ctx.set_status_info(&format!(
        "Collected indexes_to_gc for table {}, elapsed: {:?}, indexes_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&indexes_to_gc)
    ));

    let start = std::time::Instant::now();
    let subject_files_to_gc: Vec<_> = segments_to_gc
        .into_iter()
        .chain(blocks_to_gc.into_iter())
        .chain(stats_to_gc.into_iter())
        .collect();
    let op = Files::create(ctx.clone(), fuse_table.get_operator());

    // order is important
    // indexes should be removed before blocks, because index locations to gc are generated from block locations
    // subject_files should be removed before snapshots, because gc of subject_files depend on gc root
    op.remove_file_in_batch(&indexes_to_gc).await?;
    op.remove_file_in_batch(&subject_files_to_gc).await?;

    // Evict snapshot caches from the local node.
    //
    // Note:
    // - Cached snapshots may also exist on other nodes in a multi-node cluster. If these remote
    //   caches are not synchronized, it could lead to incorrect results in operations like
    //   `fuse_snapshot(...)`. However, this does not compromise the safety of the table data.
    // - TODO: To ensure correctness in such cases, the table's Least Visible Timestamp (LVT),
    //   stored in the meta-server, should be utilized to determine snapshot visibility and
    //   resolve potential issues.
    if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
        for path in snapshots_to_gc.iter() {
            snapshot_cache.evict(path);
        }
    }
    let op = Files::create(ctx.clone(), fuse_table.get_operator());
    op.remove_file_in_batch(&snapshots_to_gc).await?;

    let files_to_gc: Vec<_> = subject_files_to_gc
        .into_iter()
        .chain(snapshots_to_gc.into_iter())
        .chain(indexes_to_gc.into_iter())
        .chain(ref_info.files_to_gc.into_iter())
        .collect();
    ctx.set_status_info(&format!(
        "Removed files for table {}, elapsed: {:?}, files_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&files_to_gc)
    ));
    Ok(files_to_gc)
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
