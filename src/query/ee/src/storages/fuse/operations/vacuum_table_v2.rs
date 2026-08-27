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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::TableIndex;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::is_gc_candidate_segment_block;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use futures_util::TryStreamExt;
use log::info;
use opendal::Operator;
use opendal::Scheme;

const VACUUM2_BLOCK_DELETE_CHUNK_SIZE: usize = 1000;
const VACUUM2_SEGMENT_READ_CHUNK_SIZE: usize = 1000;

/// Block GC progress counters used for status reporting.
struct BlockGcStats {
    /// Number of candidate block objects before protected-block filtering.
    ///
    /// This excludes directory entries and entries that are not eligible by the
    /// vacuum2 cutoff/safety rule, but includes blocks that are later kept
    /// because they are still referenced by the protected block set.
    scanned_blocks: usize,
    /// Number of data block objects successfully removed.
    removed_blocks: usize,
    /// Number of objects successfully removed, including data blocks and their
    /// derived index files.
    removed_files: usize,
}

/// Shared context for block-level GC.
///
/// Keep the safety-critical inputs in one place so the FS and object-store
/// paths use the same gc-root cutoff, protected block set, and index metadata.
struct BlockGcContext<'a> {
    /// Operator used to list, stat, and delete block-related objects.
    dal: &'a Operator,
    /// Query context used for settings, abort checks, and status reporting.
    ctx: &'a Arc<dyn TableContext>,
    /// Table description used only in status/error messages.
    table_desc: &'a str,
    /// Prefix of the table block object directory, i.e. `_b/`.
    block_location_prefix: &'a str,
    /// Timestamp-derived vacuum2 object-key cutoff. Objects at or after this
    /// prefix must not be vacuumed.
    until: String,
    /// GC-root snapshot timestamp used to build `until` and for status reporting.
    gc_root_timestamp: DateTime<Utc>,
    /// GC-root object metadata timestamp used by the common vacuum safety helper
    /// for legacy object-key handling.
    gc_root_meta_ts: DateTime<Utc>,
    /// Protected data block paths that are still referenced by the gc root or refs.
    gc_root_blocks: &'a HashSet<String>,
    /// Aggregating index ids used to derive index object paths from data blocks.
    table_agg_index_ids: &'a [u64],
    /// Inverted index metadata used to derive index object paths from data blocks.
    inverted_indexes: &'a BTreeMap<String, TableIndex>,
    /// Start time of the block GC phase, used only for status reporting.
    start: std::time::Instant,
}

/// GC root context derived from the owner table before ref-aware cleanup starts.
///
/// It captures the owner table id, the selected gc-root timestamps, the snapshot
/// files currently eligible for cleanup, and the segments that must remain
/// protected while processing branch/tag references.
struct GcRootSnapshotCtx {
    gc_root_timestamp: DateTime<Utc>,
    gc_root_meta_ts: DateTime<Utc>,
    protected_segments: HashSet<Location>,
    snapshots_to_gc: Vec<String>,
}

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
    let Some(GcRootSnapshotCtx {
        gc_root_timestamp,
        gc_root_meta_ts,
        protected_segments,
        snapshots_to_gc,
    }) = vacuum_base_snapshot_phase(fuse_table, &ctx, respect_flash_back).await?
    else {
        info!("Table {} has no snapshot, stopping vacuum", table_info.desc);
        return Ok(vec![]);
    };

    let start = std::time::Instant::now();
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
    let protected_seg_paths = protected_segments
        .iter()
        .map(|(p, _)| p)
        .collect::<HashSet<_>>();
    let segments_to_gc: Vec<String> = segments_before_gc_root
        .into_iter()
        .filter(|s| !protected_seg_paths.contains(s))
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

    // Collect blocks from main gc_root. Read protected segments in chunks to avoid
    // retaining all CompactSegmentInfo objects in memory at once.
    let protected_segments = protected_segments.into_iter().collect::<Vec<_>>();
    let total_chunks = protected_segments
        .len()
        .div_ceil(VACUUM2_SEGMENT_READ_CHUNK_SIZE);
    let mut gc_root_blocks = HashSet::new();
    for (chunk_idx, segment_chunk) in protected_segments
        .chunks(VACUUM2_SEGMENT_READ_CHUNK_SIZE)
        .enumerate()
    {
        if let Err(err) = ctx.check_aborting() {
            return Err(err.with_context(format!(
                "aborted while reading protected segment chunk {}/{} for table {}",
                chunk_idx + 1,
                total_chunks,
                table_info.desc
            )));
        }

        let segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(segment_chunk, false)
            .await?;
        for segment in segments {
            gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
        }
        ctx.set_status_info(&format!(
            "Read protected segment chunk for table {}, elapsed: {:?}, segment chunk: {}/{}, segments in chunk: {}, total protected blocks: {}",
            table_info.desc,
            start.elapsed(),
            chunk_idx + 1,
            total_chunks,
            segment_chunk.len(),
            gc_root_blocks.len()
        ));
    }
    ctx.set_status_info(&format!(
        "Read segments for table {}, elapsed: {:?}, total protected blocks: {}",
        table_info.desc,
        start.elapsed(),
        gc_root_blocks.len()
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

    let mut removed_files = Vec::new();

    // order is important
    // indexes should be removed before their blocks, because index locations to gc are generated from block locations.
    let block_location_prefix = fuse_table.meta_location_generator().block_location_prefix();
    let block_gc_ctx = BlockGcContext {
        dal: fuse_table.get_operator_ref(),
        ctx: &ctx,
        table_desc: table_info.desc.as_str(),
        block_location_prefix,
        until: FuseTable::vacuum2_until_prefix(block_location_prefix, gc_root_timestamp),
        gc_root_timestamp,
        gc_root_meta_ts,
        gc_root_blocks: &gc_root_blocks,
        table_agg_index_ids: &table_agg_index_ids,
        inverted_indexes,
        start,
    };
    let block_gc_stats = purge_blocks_before_gc_root(&block_gc_ctx, &mut removed_files).await?;
    ctx.set_status_info(&format!(
        "Filtered and removed blocks for table {}, elapsed: {:?}, blocks scanned: {}, blocks removed: {}, files removed: {}",
        table_info.desc,
        start.elapsed(),
        block_gc_stats.scanned_blocks,
        block_gc_stats.removed_blocks,
        block_gc_stats.removed_files,
    ));

    let file_remover = Files::create(ctx.clone(), fuse_table.get_operator());

    // segment stats should be removed before segments.
    if !stats_to_gc.is_empty() {
        file_remover.remove_file_in_batch(&stats_to_gc).await?;
        removed_files.extend(stats_to_gc.iter().cloned());
    }

    if !segments_to_gc.is_empty() {
        file_remover.remove_file_in_batch(&segments_to_gc).await?;
        removed_files.extend(segments_to_gc.iter().cloned());
    }

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
    file_remover.remove_file_in_batch(&snapshots_to_gc).await?;
    removed_files.extend(snapshots_to_gc.iter().cloned());

    // Legacy branch/tag refs were removed without compatibility guarantees.
    // Vacuum2 cleans up the old ref snapshot prefix opportunistically, and the
    // operation is idempotent even if the prefix is already absent.
    let legacy_ref_dir = fuse_table
        .meta_location_generator()
        .ref_snapshot_location_prefix();
    let _ = fuse_table.get_operator().remove_all(legacy_ref_dir).await;

    ctx.set_status_info(&format!(
        "Removed files for table {}, elapsed: {:?}, removed_files: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&removed_files),
    ));

    Ok(removed_files)
}

async fn purge_blocks_before_gc_root(
    block_gc: &BlockGcContext<'_>,
    removed_files: &mut Vec<String>,
) -> Result<BlockGcStats> {
    info!("Listing block files until prefix: {}", block_gc.until);

    match block_gc.dal.info().scheme() {
        Scheme::Fs => purge_blocks_before_gc_root_fs(block_gc, removed_files).await,
        _ => purge_blocks_before_gc_root_object_store_streaming(block_gc, removed_files).await,
    }
}

async fn purge_blocks_before_gc_root_fs(
    block_gc: &BlockGcContext<'_>,
    removed_files: &mut Vec<String>,
) -> Result<BlockGcStats> {
    let file_remover = Files::create(Arc::clone(block_gc.ctx), block_gc.dal.clone());
    let blocks_before_gc_root = list_gc_candidate_paths_until_prefix_fs(
        block_gc.dal,
        block_gc.block_location_prefix,
        &block_gc.until,
        block_gc.gc_root_meta_ts,
    )
    .await?;
    let scanned_blocks = blocks_before_gc_root.len();
    block_gc.ctx.set_status_info(&format!(
        "Listed block paths before gc_root for table {}, elapsed: {:?}, block_location_prefix: {:?}, gc_root_timestamp: {:?}, blocks: {:?}",
        block_gc.table_desc,
        block_gc.start.elapsed(),
        block_gc.block_location_prefix,
        block_gc.gc_root_timestamp,
        slice_summary(&blocks_before_gc_root)
    ));

    let mut stats = BlockGcStats {
        scanned_blocks,
        removed_blocks: 0,
        removed_files: 0,
    };
    let mut block_chunk = Vec::with_capacity(VACUUM2_BLOCK_DELETE_CHUNK_SIZE);
    for block_path in blocks_before_gc_root {
        if !block_gc.gc_root_blocks.contains(&block_path) {
            block_chunk.push(block_path);
            if block_chunk.len() == VACUUM2_BLOCK_DELETE_CHUNK_SIZE {
                purge_block_chunk(
                    &file_remover,
                    block_gc,
                    &block_chunk,
                    removed_files,
                    &mut stats,
                )
                .await?;
                block_chunk.clear();
            }
        }
    }
    if !block_chunk.is_empty() {
        purge_block_chunk(
            &file_remover,
            block_gc,
            &block_chunk,
            removed_files,
            &mut stats,
        )
        .await?;
    }

    Ok(stats)
}

async fn purge_blocks_before_gc_root_object_store_streaming(
    block_gc: &BlockGcContext<'_>,
    removed_files: &mut Vec<String>,
) -> Result<BlockGcStats> {
    let file_remover = Files::create(Arc::clone(block_gc.ctx), block_gc.dal.clone());
    let mut lister = block_gc.dal.lister(block_gc.block_location_prefix).await?;
    let mut block_chunk = Vec::with_capacity(VACUUM2_BLOCK_DELETE_CHUNK_SIZE);
    let mut stats = BlockGcStats {
        scanned_blocks: 0,
        removed_blocks: 0,
        removed_files: 0,
    };

    block_gc.ctx.set_status_info(&format!(
        "Streaming block paths before gc_root for table {}, block_location_prefix: {:?}, gc_root_timestamp: {:?}",
        block_gc.table_desc, block_gc.block_location_prefix, block_gc.gc_root_timestamp
    ));

    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().is_dir() {
            continue;
        }

        let path = entry.path();
        if path >= block_gc.until.as_str() {
            info!("entry path: {} >= until: {}", path, block_gc.until);
            break;
        }

        if !is_gc_candidate_segment_block(&entry, block_gc.dal, block_gc.gc_root_meta_ts).await? {
            continue;
        }

        stats.scanned_blocks += 1;
        if block_gc.gc_root_blocks.contains(path) {
            continue;
        }

        block_chunk.push(path.to_owned());
        if block_chunk.len() == VACUUM2_BLOCK_DELETE_CHUNK_SIZE {
            purge_block_chunk(
                &file_remover,
                block_gc,
                &block_chunk,
                removed_files,
                &mut stats,
            )
            .await?;
            block_chunk.clear();
        }
    }

    if !block_chunk.is_empty() {
        purge_block_chunk(
            &file_remover,
            block_gc,
            &block_chunk,
            removed_files,
            &mut stats,
        )
        .await?;
    }

    Ok(stats)
}

async fn list_gc_candidate_paths_until_prefix_fs(
    dal: &Operator,
    path: &str,
    until: &str,
    gc_root_meta_ts: DateTime<Utc>,
) -> Result<Vec<String>> {
    let mut lister = dal.lister(path).await?;
    let mut entries = Vec::new();
    while let Some(item) = lister.try_next().await? {
        if item.metadata().is_file() {
            entries.push(item);
        }
    }
    entries.sort_by(|l, r| l.path().cmp(r.path()));

    let mut res = Vec::new();
    for entry in entries {
        if entry.path() >= until {
            info!("entry path: {} >= until: {}", entry.path(), until);
            break;
        }
        if is_gc_candidate_segment_block(&entry, dal, gc_root_meta_ts).await? {
            res.push(entry.path().to_owned());
        }
    }
    Ok(res)
}

async fn purge_block_chunk(
    file_remover: &Files,
    block_gc: &BlockGcContext<'_>,
    block_chunk: &[String],
    removed_files: &mut Vec<String>,
    stats: &mut BlockGcStats,
) -> Result<()> {
    if let Err(err) = block_gc.ctx.check_aborting() {
        return Err(err.with_context(format!(
            "aborted while removing block chunk for table {}, blocks removed: {}, current chunk size: {}",
            block_gc.table_desc,
            stats.removed_blocks,
            block_chunk.len()
        )));
    }

    let chunk_idx = stats.removed_blocks / VACUUM2_BLOCK_DELETE_CHUNK_SIZE + 1;
    let indexes_to_gc = collect_block_index_locations(
        block_chunk,
        block_gc.table_agg_index_ids,
        block_gc.inverted_indexes,
    );
    block_gc.ctx.set_status_info(&format!(
        "Collected indexes_to_gc for table {}, elapsed: {:?}, block chunk: {}, blocks in chunk: {}, indexes_to_gc: {:?}",
        block_gc.table_desc,
        block_gc.start.elapsed(),
        chunk_idx,
        block_chunk.len(),
        slice_summary(&indexes_to_gc)
    ));

    if !indexes_to_gc.is_empty() {
        file_remover.remove_file_in_batch(&indexes_to_gc).await?;
        stats.removed_files += indexes_to_gc.len();
        removed_files.extend(indexes_to_gc);
    }

    file_remover.remove_file_in_batch(block_chunk).await?;
    stats.removed_blocks += block_chunk.len();
    stats.removed_files += block_chunk.len();
    removed_files.extend(block_chunk.iter().cloned());

    block_gc.ctx.set_status_info(&format!(
        "Removed block chunk for table {}, elapsed: {:?}, block chunk: {}, blocks scanned: {}, blocks removed in chunk: {}, total blocks removed: {}",
        block_gc.table_desc,
        block_gc.start.elapsed(),
        chunk_idx,
        stats.scanned_blocks,
        block_chunk.len(),
        stats.removed_blocks,
    ));

    Ok(())
}

fn collect_block_index_locations(
    blocks_to_gc: &[String],
    table_agg_index_ids: &[u64],
    inverted_indexes: &BTreeMap<String, TableIndex>,
) -> Vec<String> {
    let mut indexes_to_gc = Vec::with_capacity(
        blocks_to_gc.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 1),
    );
    for loc in blocks_to_gc {
        for index_id in table_agg_index_ids {
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
    indexes_to_gc
}

#[async_backtrace::framed]
async fn vacuum_base_snapshot_phase(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Option<GcRootSnapshotCtx>> {
    let Some(mut selection) = fuse_table
        .prepare_snapshot_gc_selection(ctx, respect_flash_back)
        .await?
    else {
        return Ok(None);
    };

    let catalog = ctx
        .get_catalog(fuse_table.get_table_info().catalog())
        .await?;
    let mut protected_segments = selection
        .gc_root
        .segments
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let _ = fuse_table
        .process_tags_for_purge(
            &catalog,
            &selection.gc_root_path,
            &mut selection.snapshots_to_gc,
            &mut protected_segments,
            false,
        )
        .await?;

    Ok(Some(GcRootSnapshotCtx {
        gc_root_timestamp: selection.gc_root.timestamp.unwrap(),
        gc_root_meta_ts: selection.gc_root_meta_ts,
        protected_segments,
        snapshots_to_gc: selection.snapshots_to_gc,
    }))
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

#[cfg(test)]
mod tests {
    use databend_common_meta_app::schema::TableIndexType;

    use super::*;

    #[test]
    fn test_collect_block_index_locations_keeps_per_block_order() {
        let blocks = vec![
            "1/2/_b/g0123456789abcdef0123456789abcdef_v2.parquet".to_string(),
            "1/2/_b/hfedcba9876543210fedcba9876543210_v2.parquet".to_string(),
        ];
        let mut inverted_indexes = BTreeMap::new();
        inverted_indexes.insert("idx".to_string(), TableIndex {
            index_type: TableIndexType::Inverted,
            name: "idx".to_string(),
            column_ids: vec![0],
            sync_creation: true,
            version: "123456789".to_string(),
            options: BTreeMap::new(),
        });

        let indexes = collect_block_index_locations(&blocks, &[7], &inverted_indexes);

        assert_eq!(indexes, vec![
            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&blocks[0], 7),
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &blocks[0],
                "idx",
                "123456789",
            ),
            TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(&blocks[0]),
            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&blocks[1], 7),
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &blocks[1],
                "idx",
                "123456789",
            ),
            TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(&blocks[1]),
        ]);
    }
}
