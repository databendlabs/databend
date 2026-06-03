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
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_io::dedup_file_locations;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use log::info;

const VACUUM2_TARGET_INDEX_FILES_PER_CHUNK: usize = 100_000;
const VACUUM2_MIN_BLOCKS_PER_CHUNK: usize = 1_000;
const VACUUM2_MAX_BLOCKS_PER_CHUNK: usize = 100_000;

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

    // Collect blocks from main gc_root
    let protected_segments = protected_segments.into_iter().collect::<Vec<_>>();
    let segments = segments_io
        .read_segments::<Arc<CompactSegmentInfo>>(&protected_segments, false)
        .await?;
    let mut gc_root_blocks = HashSet::new();
    for segment in segments {
        gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
    }
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

    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &table_info.meta.indexes;
    let index_files_per_block = table_agg_index_ids.len() + inverted_indexes.len() + 1;
    let block_chunk_size = compute_vacuum2_block_chunk_size(index_files_per_block);
    ctx.set_status_info(&format!(
        "Collected index metadata for table {}, agg_indexes: {}, inverted_indexes: {}, block_chunk_size: {}",
        table_info.desc,
        table_agg_index_ids.len(),
        inverted_indexes.len(),
        block_chunk_size
    ));

    let start = std::time::Instant::now();
    let mut remover =
        FuseVacuum2FileRemover::new(Files::create(ctx.clone(), fuse_table.get_operator()));

    // Delete block-derived files by chunks so a failed vacuum can still make durable progress:
    // each completed chunk removes indexes first, then removes the blocks they were derived from.
    let delete_stats = remove_vacuum2_files(
        &mut remover,
        &table_info.desc,
        &blocks_to_gc,
        &segments_to_gc,
        &stats_to_gc,
        &snapshots_to_gc,
        block_chunk_size,
        &table_agg_index_ids,
        inverted_indexes,
        |status| ctx.set_status_info(&status),
        || {
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
        },
    )
    .await?;

    // Legacy branch/tag refs were removed without compatibility guarantees.
    // Vacuum2 cleans up the old ref snapshot prefix opportunistically, and the
    // operation is idempotent even if the prefix is already absent.
    let legacy_ref_dir = fuse_table
        .meta_location_generator()
        .ref_snapshot_location_prefix();
    let _ = fuse_table.get_operator().remove_all(legacy_ref_dir).await;

    let summary = delete_stats.summary(&table_info.desc);
    ctx.set_status_info(&format!(
        "Removed files for table {}, elapsed: {:?}, {}",
        table_info.desc,
        start.elapsed(),
        summary,
    ));

    Ok(vec![summary])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Vacuum2DeletePhase {
    Indexes,
    Blocks,
    SegmentStats,
    Segments,
    Snapshots,
}

impl Vacuum2DeletePhase {
    fn as_str(self) -> &'static str {
        match self {
            Vacuum2DeletePhase::Indexes => "indexes",
            Vacuum2DeletePhase::Blocks => "blocks",
            Vacuum2DeletePhase::SegmentStats => "segment_stats",
            Vacuum2DeletePhase::Segments => "segments",
            Vacuum2DeletePhase::Snapshots => "snapshots",
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Vacuum2DeleteStats {
    block_chunks: usize,
    blocks: usize,
    index_files: usize,
    segment_stats: usize,
    segments: usize,
    snapshots: usize,
}

impl Vacuum2DeleteStats {
    fn summary(&self, table_desc: &str) -> String {
        format!(
            "table={}, block_chunks={}, blocks={}, index_files={}, segment_stats={}, segments={}, snapshots={}",
            table_desc,
            self.block_chunks,
            self.blocks,
            self.index_files,
            self.segment_stats,
            self.segments,
            self.snapshots
        )
    }
}

#[async_trait::async_trait]
trait Vacuum2FileRemover: Send {
    async fn remove_files(
        &mut self,
        phase: Vacuum2DeletePhase,
        file_locations: &[String],
    ) -> Result<()>;
}

struct FuseVacuum2FileRemover {
    files: Files,
}

impl FuseVacuum2FileRemover {
    fn new(files: Files) -> Self {
        Self { files }
    }
}

#[async_trait::async_trait]
impl Vacuum2FileRemover for FuseVacuum2FileRemover {
    async fn remove_files(
        &mut self,
        _phase: Vacuum2DeletePhase,
        file_locations: &[String],
    ) -> Result<()> {
        self.files.remove_file_in_batch(file_locations).await
    }
}

#[allow(clippy::too_many_arguments)]
async fn remove_vacuum2_files<R, S, E>(
    remover: &mut R,
    table_desc: &str,
    blocks_to_gc: &[String],
    segments_to_gc: &[String],
    stats_to_gc: &[String],
    snapshots_to_gc: &[String],
    block_chunk_size: usize,
    table_agg_index_ids: &[u64],
    inverted_indexes: &BTreeMap<String, TableIndex>,
    mut set_status: S,
    mut before_snapshots: E,
) -> Result<Vacuum2DeleteStats>
where
    R: Vacuum2FileRemover,
    S: FnMut(String) + Send,
    E: FnMut() + Send,
{
    let start = std::time::Instant::now();
    let block_chunk_size = block_chunk_size.max(1);
    let total_block_chunks = blocks_to_gc.len().div_ceil(block_chunk_size);
    let mut stats = Vacuum2DeleteStats {
        block_chunks: total_block_chunks,
        ..Default::default()
    };

    for (idx, block_chunk) in blocks_to_gc.chunks(block_chunk_size).enumerate() {
        let index_files =
            build_index_locations_for_blocks(block_chunk, table_agg_index_ids, inverted_indexes);

        set_status(format!(
            "Deleting block chunk for table {}, progress: {}/{}, elapsed: {:?}, block_files: {}, index_files: {}",
            table_desc,
            idx + 1,
            total_block_chunks,
            start.elapsed(),
            block_chunk.len(),
            index_files.len()
        ));
        info!(
            "Deleting block chunk for table {}, progress: {}/{}, block_files: {}, index_files: {}",
            table_desc,
            idx + 1,
            total_block_chunks,
            block_chunk.len(),
            index_files.len()
        );

        remove_non_empty(remover, Vacuum2DeletePhase::Indexes, &index_files).await?;
        stats.index_files += index_files.len();
        remove_non_empty(remover, Vacuum2DeletePhase::Blocks, block_chunk).await?;
        stats.blocks += block_chunk.len();

        set_status(format!(
            "Deleted block chunk for table {}, progress: {}/{}, elapsed: {:?}, deleted_blocks: {}, deleted_index_files: {}",
            table_desc,
            idx + 1,
            total_block_chunks,
            start.elapsed(),
            stats.blocks,
            stats.index_files
        ));
    }

    // Stats paths are derived from segment paths, so delete stats before segments.
    remove_non_empty(remover, Vacuum2DeletePhase::SegmentStats, stats_to_gc).await?;
    stats.segment_stats = stats_to_gc.len();
    remove_non_empty(remover, Vacuum2DeletePhase::Segments, segments_to_gc).await?;
    stats.segments = segments_to_gc.len();

    before_snapshots();
    remove_non_empty(remover, Vacuum2DeletePhase::Snapshots, snapshots_to_gc).await?;
    stats.snapshots = snapshots_to_gc.len();

    Ok(stats)
}

async fn remove_non_empty<R: Vacuum2FileRemover>(
    remover: &mut R,
    phase: Vacuum2DeletePhase,
    file_locations: &[String],
) -> Result<()> {
    if file_locations.is_empty() {
        return Ok(());
    }

    info!(
        "Removing vacuum2 {} files, count: {}",
        phase.as_str(),
        file_locations.len()
    );
    remover.remove_files(phase, file_locations).await
}

fn build_index_locations_for_blocks(
    blocks: &[String],
    table_agg_index_ids: &[u64],
    inverted_indexes: &BTreeMap<String, TableIndex>,
) -> Vec<String> {
    let mut index_locations =
        Vec::with_capacity(blocks.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 1));
    for loc in blocks {
        for index_id in table_agg_index_ids {
            index_locations.push(
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                ),
            );
        }
        for idx in inverted_indexes.values() {
            index_locations.push(
                TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                    loc,
                    idx.name.as_str(),
                    idx.version.as_str(),
                ),
            );
        }
        index_locations
            .push(TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc));
    }

    dedup_file_locations(&mut index_locations);
    index_locations
}

fn compute_vacuum2_block_chunk_size(index_files_per_block: usize) -> usize {
    let index_files_per_block = index_files_per_block.max(1);
    (VACUUM2_TARGET_INDEX_FILES_PER_CHUNK / index_files_per_block)
        .clamp(VACUUM2_MIN_BLOCKS_PER_CHUNK, VACUUM2_MAX_BLOCKS_PER_CHUNK)
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
    use databend_common_exception::ErrorCode;
    use databend_common_meta_app::schema::TableIndexType;

    use super::*;

    fn block_location(id: &str) -> String {
        format!("1/2/_b/{}_v4.parquet", id)
    }

    fn inverted_index(name: &str, version: &str) -> TableIndex {
        TableIndex {
            index_type: TableIndexType::Inverted,
            name: name.to_owned(),
            column_ids: vec![0],
            sync_creation: true,
            version: version.to_owned(),
            options: BTreeMap::new(),
        }
    }

    #[test]
    fn test_compute_vacuum2_block_chunk_size() {
        assert_eq!(compute_vacuum2_block_chunk_size(0), 100_000);
        assert_eq!(compute_vacuum2_block_chunk_size(1), 100_000);
        assert_eq!(compute_vacuum2_block_chunk_size(2), 50_000);
        assert_eq!(compute_vacuum2_block_chunk_size(101), 1_000);
    }

    #[test]
    fn test_build_index_locations_for_blocks() {
        let blocks = vec![block_location("0123456789abcdef0123456789abcdef")];
        let agg_index_ids = vec![7, 9];
        let inverted_indexes =
            BTreeMap::from([("idx".to_owned(), inverted_index("idx", "1234567890abcdef"))]);

        let index_locations =
            build_index_locations_for_blocks(&blocks, &agg_index_ids, &inverted_indexes);

        assert_eq!(index_locations.len(), 4);
        assert!(index_locations.contains(
            &TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&blocks[0], 7)
        ));
        assert!(index_locations.contains(
            &TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&blocks[0], 9)
        ));
        assert!(index_locations.contains(
            &TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &blocks[0],
                "idx",
                "1234567890abcdef",
            )
        ));
        assert!(index_locations.contains(
            &TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(&blocks[0])
        ));
    }

    #[derive(Default)]
    struct RecordingRemover {
        calls: Vec<(Vacuum2DeletePhase, Vec<String>)>,
        fail_at_call: Option<usize>,
    }

    #[async_trait::async_trait]
    impl Vacuum2FileRemover for RecordingRemover {
        async fn remove_files(
            &mut self,
            phase: Vacuum2DeletePhase,
            file_locations: &[String],
        ) -> Result<()> {
            self.calls.push((phase, file_locations.to_vec()));
            if self.fail_at_call == Some(self.calls.len()) {
                return Err(ErrorCode::StorageOther(
                    "injected vacuum2 delete failure".to_owned(),
                ));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_remove_vacuum2_files_deletes_blocks_after_each_index_chunk() {
        let blocks = vec![
            block_location("0123456789abcdef0123456789abcdef"),
            block_location("1123456789abcdef0123456789abcdef"),
            block_location("2123456789abcdef0123456789abcdef"),
        ];
        let stats_files = vec!["1/2/_sg_st/0123456789abcdef0123456789abcdef_v4.mpk".to_owned()];
        let segment_files = vec!["1/2/_sg/0123456789abcdef0123456789abcdef_v4.json".to_owned()];
        let snapshot_files = vec!["1/2/_ss/0123456789abcdef0123456789abcdef_v4.json".to_owned()];
        let inverted_indexes = BTreeMap::new();

        let mut remover = RecordingRemover::default();
        let delete_stats = remove_vacuum2_files(
            &mut remover,
            "`default`.`t`",
            &blocks,
            &segment_files,
            &stats_files,
            &snapshot_files,
            2,
            &[],
            &inverted_indexes,
            |_| {},
            || {},
        )
        .await
        .unwrap();

        assert_eq!(delete_stats, Vacuum2DeleteStats {
            block_chunks: 2,
            blocks: 3,
            index_files: 3,
            segment_stats: 1,
            segments: 1,
            snapshots: 1,
        });
        assert_eq!(
            remover
                .calls
                .iter()
                .map(|(phase, _)| *phase)
                .collect::<Vec<_>>(),
            vec![
                Vacuum2DeletePhase::Indexes,
                Vacuum2DeletePhase::Blocks,
                Vacuum2DeletePhase::Indexes,
                Vacuum2DeletePhase::Blocks,
                Vacuum2DeletePhase::SegmentStats,
                Vacuum2DeletePhase::Segments,
                Vacuum2DeletePhase::Snapshots,
            ]
        );
        assert_eq!(remover.calls[1].1, blocks[..2]);
        assert_eq!(remover.calls[3].1, blocks[2..]);
    }

    #[tokio::test]
    async fn test_remove_vacuum2_files_keeps_completed_block_chunk_on_later_failure() {
        let blocks = vec![
            block_location("0123456789abcdef0123456789abcdef"),
            block_location("1123456789abcdef0123456789abcdef"),
            block_location("2123456789abcdef0123456789abcdef"),
        ];
        let inverted_indexes = BTreeMap::new();
        let mut remover = RecordingRemover {
            fail_at_call: Some(3),
            ..Default::default()
        };

        let result = remove_vacuum2_files(
            &mut remover,
            "`default`.`t`",
            &blocks,
            &[],
            &[],
            &[],
            2,
            &[],
            &inverted_indexes,
            |_| {},
            || {},
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            remover
                .calls
                .iter()
                .map(|(phase, _)| *phase)
                .collect::<Vec<_>>(),
            vec![
                Vacuum2DeletePhase::Indexes,
                Vacuum2DeletePhase::Blocks,
                Vacuum2DeletePhase::Indexes,
            ]
        );
        assert_eq!(remover.calls[1].1, blocks[..2]);
    }
}
