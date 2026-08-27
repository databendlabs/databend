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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::TableIndex;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ASSUMPTION_MAX_TXN_DURATION;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::meta::uuid_from_date_time;
use futures_util::TryStreamExt;
use log::info;
use opendal::Entry;
use opendal::Operator;
use opendal::Scheme;

const VACUUM2_BLOCK_DELETE_CHUNK_SIZE: usize = 1000;
const VACUUM2_SEGMENT_READ_CHUNK_SIZE: usize = 1000;
const VACUUM2_REMOVED_FILES_RESULT_LIMIT: usize = 1000;

#[derive(Default)]
struct RemovedFilesCollector {
    files: Vec<String>,
    omitted: usize,
}

impl RemovedFilesCollector {
    fn record(&mut self, file: String) {
        if self.files.len() < VACUUM2_REMOVED_FILES_RESULT_LIMIT {
            self.files.push(file);
        } else {
            self.omitted += 1;
        }
    }

    fn record_many(&mut self, files: impl IntoIterator<Item = String>) {
        for file in files {
            self.record(file);
        }
    }

    fn record_many_cloned<'a>(&mut self, files: impl IntoIterator<Item = &'a String>) {
        for file in files {
            self.record(file.clone());
        }
    }

    fn summary(&self) -> String {
        if self.omitted == 0 {
            slice_summary(&self.files)
        } else {
            format!(
                "{}, omitted additional removed files from result: {}",
                slice_summary(&self.files),
                self.omitted
            )
        }
    }

    fn into_result(mut self) -> Vec<String> {
        if self.omitted > 0 {
            self.files.push(format!(
                "... omitted {} additional removed files from result to avoid excessive memory usage",
                self.omitted
            ));
        }
        self.files
    }
}

struct BlockGcStats {
    scanned_blocks: usize,
    removed_blocks: usize,
    removed_files: usize,
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

    let op = Files::create(ctx.clone(), fuse_table.get_operator());
    let mut files_to_gc = RemovedFilesCollector::default();

    // order is important
    // indexes should be removed before their blocks, because index locations to gc are generated from block locations.
    let block_gc_stats = purge_blocks_before_gc_root_streaming(
        fuse_table.get_operator_ref(),
        &op,
        &ctx,
        table_info.desc.as_str(),
        fuse_table.meta_location_generator().block_location_prefix(),
        gc_root_timestamp,
        gc_root_meta_ts,
        &gc_root_blocks,
        &table_agg_index_ids,
        inverted_indexes,
        &mut files_to_gc,
        start,
    )
    .await?;
    ctx.set_status_info(&format!(
        "Filtered and removed blocks for table {}, elapsed: {:?}, blocks scanned: {}, blocks removed: {}, files removed: {}",
        table_info.desc,
        start.elapsed(),
        block_gc_stats.scanned_blocks,
        block_gc_stats.removed_blocks,
        block_gc_stats.removed_files,
    ));

    // segment stats should be removed before segments.
    if !stats_to_gc.is_empty() {
        op.remove_file_in_batch(&stats_to_gc).await?;
        files_to_gc.record_many_cloned(stats_to_gc.iter());
    }

    if !segments_to_gc.is_empty() {
        op.remove_file_in_batch(&segments_to_gc).await?;
        files_to_gc.record_many_cloned(segments_to_gc.iter());
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
    op.remove_file_in_batch(&snapshots_to_gc).await?;
    files_to_gc.record_many_cloned(snapshots_to_gc.iter());

    // Legacy branch/tag refs were removed without compatibility guarantees.
    // Vacuum2 cleans up the old ref snapshot prefix opportunistically, and the
    // operation is idempotent even if the prefix is already absent.
    let legacy_ref_dir = fuse_table
        .meta_location_generator()
        .ref_snapshot_location_prefix();
    let _ = fuse_table.get_operator().remove_all(legacy_ref_dir).await;

    ctx.set_status_info(&format!(
        "Removed files for table {}, elapsed: {:?}, files_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        files_to_gc.summary(),
    ));

    Ok(files_to_gc.into_result())
}

#[allow(clippy::too_many_arguments)]
async fn purge_blocks_before_gc_root_streaming(
    dal: &Operator,
    op: &Files,
    ctx: &Arc<dyn TableContext>,
    table_desc: &str,
    block_dir: &str,
    gc_root_timestamp: DateTime<Utc>,
    gc_root_meta_ts: DateTime<Utc>,
    gc_root_blocks: &HashSet<String>,
    table_agg_index_ids: &[u64],
    inverted_indexes: &BTreeMap<String, TableIndex>,
    files_to_gc: &mut RemovedFilesCollector,
    start: std::time::Instant,
) -> Result<BlockGcStats> {
    let until = vacuum2_until_prefix(block_dir, gc_root_timestamp);
    info!("Listing block files until prefix: {}", until);

    match dal.info().scheme() {
        Scheme::Fs => {
            let blocks_before_gc_root =
                list_gc_candidate_paths_until_prefix_fs(dal, block_dir, &until, gc_root_meta_ts)
                    .await?;
            let scanned_blocks = blocks_before_gc_root.len();
            ctx.set_status_info(&format!(
                "Listed block paths before gc_root for table {}, elapsed: {:?}, block_dir: {:?}, gc_root_timestamp: {:?}, blocks: {:?}",
                table_desc,
                start.elapsed(),
                block_dir,
                gc_root_timestamp,
                slice_summary(&blocks_before_gc_root)
            ));

            let mut stats = BlockGcStats {
                scanned_blocks,
                removed_blocks: 0,
                removed_files: 0,
            };
            let mut block_chunk = Vec::with_capacity(VACUUM2_BLOCK_DELETE_CHUNK_SIZE);
            for block_path in blocks_before_gc_root {
                if should_remove_candidate_block(&block_path, gc_root_blocks) {
                    block_chunk.push(block_path);
                    if block_chunk.len() == VACUUM2_BLOCK_DELETE_CHUNK_SIZE {
                        purge_block_chunk(
                            op,
                            ctx,
                            table_desc,
                            &block_chunk,
                            table_agg_index_ids,
                            inverted_indexes,
                            files_to_gc,
                            start,
                            &mut stats,
                        )
                        .await?;
                        block_chunk.clear();
                    }
                }
            }
            if !block_chunk.is_empty() {
                purge_block_chunk(
                    op,
                    ctx,
                    table_desc,
                    &block_chunk,
                    table_agg_index_ids,
                    inverted_indexes,
                    files_to_gc,
                    start,
                    &mut stats,
                )
                .await?;
            }
            Ok(stats)
        }
        _ => {
            let mut lister = dal.lister(block_dir).await?;
            let mut block_chunk = Vec::with_capacity(VACUUM2_BLOCK_DELETE_CHUNK_SIZE);
            let mut stats = BlockGcStats {
                scanned_blocks: 0,
                removed_blocks: 0,
                removed_files: 0,
            };

            while let Some(entry) = lister.try_next().await? {
                if entry.metadata().is_dir() {
                    continue;
                }

                let path = entry.path();
                if path >= until.as_str() {
                    info!("entry path: {} >= until: {}", path, until);
                    break;
                }

                if !is_gc_candidate_segment_block(&entry, dal, gc_root_meta_ts).await? {
                    continue;
                }

                stats.scanned_blocks += 1;
                if !should_remove_candidate_block(path, gc_root_blocks) {
                    continue;
                }

                block_chunk.push(path.to_owned());
                if block_chunk.len() == VACUUM2_BLOCK_DELETE_CHUNK_SIZE {
                    purge_block_chunk(
                        op,
                        ctx,
                        table_desc,
                        &block_chunk,
                        table_agg_index_ids,
                        inverted_indexes,
                        files_to_gc,
                        start,
                        &mut stats,
                    )
                    .await?;
                    block_chunk.clear();
                }
            }

            if !block_chunk.is_empty() {
                purge_block_chunk(
                    op,
                    ctx,
                    table_desc,
                    &block_chunk,
                    table_agg_index_ids,
                    inverted_indexes,
                    files_to_gc,
                    start,
                    &mut stats,
                )
                .await?;
            }
            Ok(stats)
        }
    }
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

fn vacuum2_until_prefix(path: &str, until: DateTime<Utc>) -> String {
    let uuid = uuid_from_date_time(until);
    let uuid_str = uuid.simple().to_string();

    // extract the most significant 48 bits, which is 12 characters
    let timestamp_component = &uuid_str[..12];
    format!(
        "{}{}{}",
        path, VACUUM2_OBJECT_KEY_PREFIX, timestamp_component
    )
}

/// Check if an entry is a candidate for garbage collection.
///
/// This mirrors the vacuum2 safety rule in databend-common-storages-fuse: new
/// vacuum2 object keys are timestamp-ordered and safe to classify by key, while
/// legacy keys require object last_modified to be older than the gc-root object
/// by ASSUMPTION_MAX_TXN_DURATION. If last_modified is unavailable from listing,
/// stat the object instead of guessing; failing to get it aborts vacuum instead
/// of risking data loss.
async fn is_gc_candidate_segment_block(
    entry: &Entry,
    op: &Operator,
    gc_root_meta_ts: DateTime<Utc>,
) -> Result<bool> {
    is_gc_candidate_segment_block_by_path_and_last_modified(
        entry.path(),
        entry.metadata().last_modified(),
        || async {
            op.stat(entry.path()).await?.last_modified().ok_or_else(|| {
                ErrorCode::StorageOther(format!(
                    "Failed to get `last_modified` metadata of the entry '{}'",
                    entry.path()
                ))
            })
        },
        gc_root_meta_ts,
    )
    .await
}

async fn is_gc_candidate_segment_block_by_path_and_last_modified<F, Fut>(
    path: &str,
    last_modified: Option<DateTime<Utc>>,
    stat_last_modified: F,
    gc_root_meta_ts: DateTime<Utc>,
) -> Result<bool>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<DateTime<Utc>>>,
{
    let last_part = path.rsplit('/').next().unwrap();
    if last_part.starts_with(VACUUM2_OBJECT_KEY_PREFIX) {
        return Ok(true);
    }

    let last_modified = match last_modified {
        Some(v) => v,
        None => stat_last_modified().await?,
    };
    Ok(last_modified + ASSUMPTION_MAX_TXN_DURATION < gc_root_meta_ts)
}

fn should_remove_candidate_block(path: &str, gc_root_blocks: &HashSet<String>) -> bool {
    !gc_root_blocks.contains(path)
}

#[allow(clippy::too_many_arguments)]
async fn purge_block_chunk(
    op: &Files,
    ctx: &Arc<dyn TableContext>,
    table_desc: &str,
    block_chunk: &[String],
    table_agg_index_ids: &[u64],
    inverted_indexes: &BTreeMap<String, TableIndex>,
    files_to_gc: &mut RemovedFilesCollector,
    start: std::time::Instant,
    stats: &mut BlockGcStats,
) -> Result<()> {
    if let Err(err) = ctx.check_aborting() {
        return Err(err.with_context(format!(
            "aborted while removing block chunk for table {}, blocks removed: {}, current chunk size: {}",
            table_desc,
            stats.removed_blocks,
            block_chunk.len()
        )));
    }

    let chunk_idx = stats.removed_blocks / VACUUM2_BLOCK_DELETE_CHUNK_SIZE + 1;
    let indexes_to_gc =
        collect_block_index_locations(block_chunk, table_agg_index_ids, inverted_indexes);
    ctx.set_status_info(&format!(
        "Collected indexes_to_gc for table {}, elapsed: {:?}, block chunk: {}, blocks in chunk: {}, indexes_to_gc: {:?}",
        table_desc,
        start.elapsed(),
        chunk_idx,
        block_chunk.len(),
        slice_summary(&indexes_to_gc)
    ));

    if !indexes_to_gc.is_empty() {
        op.remove_file_in_batch(&indexes_to_gc).await?;
        stats.removed_files += indexes_to_gc.len();
        files_to_gc.record_many(indexes_to_gc);
    }

    op.remove_file_in_batch(block_chunk).await?;
    stats.removed_blocks += block_chunk.len();
    stats.removed_files += block_chunk.len();
    files_to_gc.record_many_cloned(block_chunk.iter());

    ctx.set_status_info(&format!(
        "Removed block chunk for table {}, elapsed: {:?}, block chunk: {}, blocks scanned: {}, blocks removed in chunk: {}, total blocks removed: {}",
        table_desc,
        start.elapsed(),
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use chrono::Duration;
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

    #[test]
    fn test_removed_files_collector_caps_result_memory() {
        let mut collector = RemovedFilesCollector::default();
        collector
            .record_many((0..VACUUM2_REMOVED_FILES_RESULT_LIMIT + 2).map(|i| format!("file-{i}")));

        assert_eq!(collector.files.len(), VACUUM2_REMOVED_FILES_RESULT_LIMIT);
        assert_eq!(collector.omitted, 2);
        assert!(
            collector
                .summary()
                .contains("omitted additional removed files")
        );

        let result = collector.into_result();
        assert_eq!(result.len(), VACUUM2_REMOVED_FILES_RESULT_LIMIT + 1);
        assert!(
            result
                .last()
                .unwrap()
                .contains("omitted 2 additional removed files")
        );
    }

    #[tokio::test]
    async fn test_vacuum2_gc_candidate_rules_keep_protected_and_newer_legacy_objects() {
        let gc_root_meta_ts = Utc::now();
        let old_legacy_ts = gc_root_meta_ts - ASSUMPTION_MAX_TXN_DURATION - Duration::seconds(1);
        let new_legacy_ts = gc_root_meta_ts - ASSUMPTION_MAX_TXN_DURATION;
        let stat_calls = AtomicUsize::new(0);

        assert!(
            is_gc_candidate_segment_block_by_path_and_last_modified(
                "1/2/_b/h01abcdef_v2.parquet",
                None,
                || async {
                    stat_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(new_legacy_ts)
                },
                gc_root_meta_ts,
            )
            .await
            .unwrap()
        );
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);

        assert!(
            is_gc_candidate_segment_block_by_path_and_last_modified(
                "1/2/_b/g01abcdef_v2.parquet",
                Some(old_legacy_ts),
                || async {
                    stat_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(new_legacy_ts)
                },
                gc_root_meta_ts,
            )
            .await
            .unwrap()
        );
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);

        assert!(
            !is_gc_candidate_segment_block_by_path_and_last_modified(
                "1/2/_b/g01abcdef_v2.parquet",
                Some(new_legacy_ts),
                || async {
                    stat_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(old_legacy_ts)
                },
                gc_root_meta_ts,
            )
            .await
            .unwrap()
        );
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);

        let stat_candidate = is_gc_candidate_segment_block_by_path_and_last_modified(
            "1/2/_b/g01abcdef_v2.parquet",
            None,
            || async {
                stat_calls.fetch_add(1, Ordering::SeqCst);
                Ok(old_legacy_ts)
            },
            gc_root_meta_ts,
        )
        .await
        .unwrap();
        assert!(stat_candidate);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_vacuum2_gc_candidate_stat_error_is_not_ignored() {
        let gc_root_meta_ts = Utc::now();
        let err = is_gc_candidate_segment_block_by_path_and_last_modified(
            "1/2/_b/g01abcdef_v2.parquet",
            None,
            || async { Err(ErrorCode::StorageOther("stat failed")) },
            gc_root_meta_ts,
        )
        .await
        .unwrap_err();

        assert_eq!(err.code(), ErrorCode::STORAGE_OTHER);
    }

    #[test]
    fn test_vacuum2_block_filter_never_removes_protected_blocks() {
        let protected_blocks = HashSet::from([
            "1/2/_b/h01protected_v2.parquet".to_string(),
            "1/2/_b/g01legacy_protected_v2.parquet".to_string(),
        ]);

        assert!(!should_remove_candidate_block(
            "1/2/_b/h01protected_v2.parquet",
            &protected_blocks
        ));
        assert!(!should_remove_candidate_block(
            "1/2/_b/g01legacy_protected_v2.parquet",
            &protected_blocks
        ));
        assert!(should_remove_candidate_block(
            "1/2/_b/h01unprotected_v2.parquet",
            &protected_blocks
        ));
    }
}
