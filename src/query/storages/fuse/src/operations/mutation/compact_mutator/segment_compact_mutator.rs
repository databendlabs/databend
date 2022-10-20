//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use metrics::counter;
use metrics::gauge;
use opendal::Operator;
use tracing::info;
use tracing::warn;

use crate::io::Files;
use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::commit::Conflict;
use crate::operations::commit::MutatorConflictDetector;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::reducers::reduce_statistics;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;

const MAX_RETRIES: u64 = 10;
pub struct SegmentCompactMutator {
    ctx: Arc<dyn TableContext>,
    // the snapshot that compactor working on, it never changed during phases compaction.
    base_snapshot: Arc<TableSnapshot>,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,
    blocks_per_seg: usize,
    // chosen from the base_snapshot.segments which contains number of
    // blocks lesser than `blocks_per_seg`, and then compacted them into `compacted_segments`,
    // such that, at most one of them contains less than `blocks_per_seg` number of blocks,
    // and each of the others(if any) contains `blocks_per_seg` number of blocks.
    compacted_segment_accumulator: SegmentAccumulator,
    // segments of base_snapshot that need not to be compacted, but should be included in
    // the new snapshot
    unchanged_segment_accumulator: SegmentAccumulator,
}

struct SegmentAccumulator {
    // location of accumulated segments
    locations: Vec<Location>,
    // summarised statistics of all the accumulated segment
    summary: Statistics,
}

impl SegmentAccumulator {
    fn new() -> Self {
        Self {
            locations: vec![],
            summary: Default::default(),
        }
    }

    fn merge_stats(&mut self, stats: &Statistics) -> Result<()> {
        self.summary = merge_statistics(stats, &self.summary)?;
        Ok(())
    }

    fn add_location(&mut self, location: Location) {
        self.locations.push(location)
    }
}

impl SegmentCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        location_generator: TableMetaLocationGenerator,
        blocks_per_seg: usize,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            base_snapshot,
            data_accessor: operator,
            location_generator,
            blocks_per_seg,
            compacted_segment_accumulator: SegmentAccumulator::new(),
            unchanged_segment_accumulator: SegmentAccumulator::new(),
        })
    }

    fn need_compaction(&self) -> bool {
        !self.compacted_segment_accumulator.locations.is_empty()
    }
}

// The rough idea goes like this:
// 1. `target_select` will working and the base_snapshot: compact segments and keep the compactions
// 2. `try_commit` will try to commit the compacted snapshot to the meta store.
//     in condition of reconcilable conflicts detected, compactor will try to resolve the conflict and try again.
//     after MAX_RETRIES attempts, if still failed, abort the operation even if conflict resolvable.
//
// example of a merge process:
// - segments of base_snapshot:
//  [a, b, c, d, e, f, g]
//  suppose a and d are compacted enough, and others are not.
// - after `select_target`:
//  unchanged_segments : [a, d]
//  compacted_segments : [x, y], where
//     [b, c] compacted into x
//     [e, f, g] compacted into y
// - if there is a concurrent tx, committed BEFORE compact segments,
//   and suppose the segments of the tx's snapshot is
//  [a, b, c, d, e, f, g, h, i]
//  compare with the base_snapshot's segments
//  [a, b, c, d, e, f, g]
// we know that tx is appended(only) on the base of base_snapshot
// and the `appended_segments` is [h, i]
//
// The final merged segments should be
// [x, y, a, d, h, i]
//
// note that
// 1. the concurrently appended(and committed) segment will NOT be compacted here, to make the commit of compact agile
// 2. in the implementation, the order of segment in the vector is arranged in reversed order
//    compaction starts from the head of the snapshot's segment vector.
//    newly compacted segments will be added the end of snapshot's segment list, so that later
//    when compaction of segments is executed incrementally, the newly added segments will be
//    checked first.
//

#[async_trait::async_trait]
impl TableMutator for SegmentCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let select_begin = Instant::now();

        let fuse_segment_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let base_segment_locations = &self.base_snapshot.segments;
        let base_segments = fuse_segment_io
            .read_segments(&self.base_snapshot.segments)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let blocks_per_segment_threshold = self.blocks_per_seg;

        let mut segments_tobe_compacted = Vec::with_capacity(base_segments.len() / 2);
        for (idx, segment) in base_segments.iter().enumerate() {
            let number_blocks = segment.blocks.len();
            if number_blocks >= blocks_per_segment_threshold {
                // skip if current segment is large enough, mark it as unchanged
                self.unchanged_segment_accumulator
                    .add_location(base_segment_locations[idx].clone());
                self.unchanged_segment_accumulator
                    .merge_stats(&segment.summary)?;
                continue;
            } else {
                // if number of blocks meets the threshold, mark them down
                segments_tobe_compacted.push(segment)
            }
        }

        // check if necessary to compact the segment meta: at least two segments were marked as segments_tobe_compacted
        if segments_tobe_compacted.len() <= 1 {
            return Ok(false);
        }

        // flatten the block metas of segments being compacted
        let mut blocks_of_new_segments: Vec<&BlockMeta> = vec![];
        for segment in segments_tobe_compacted {
            for x in &segment.blocks {
                blocks_of_new_segments.push(x);
            }
        }

        // split the block metas into chunks of blocks, with chunk size set to blocks_per_seg
        let chunk_of_blocks = blocks_of_new_segments.chunks(self.blocks_per_seg);
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let segment_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );

        // Build new segments which are compacted according to the setting of `block_per_seg`
        // note that the newly segments will be persistent into storage, such that if retry
        // happens during `try_commit`, they do no need to be written again.
        for chunk in chunk_of_blocks {
            let stats = reduce_block_metas(chunk)?;
            self.compacted_segment_accumulator.merge_stats(&stats)?;
            let blocks: Vec<BlockMeta> = chunk.iter().map(|block| Clone::clone(*block)).collect();
            let new_segment = SegmentInfo::new(blocks, stats);
            let location = segment_writer.write_segment(new_segment).await?;
            self.compacted_segment_accumulator.add_location(location)
        }

        gauge!(
            "fuse_compact_segments_select_duration_second",
            select_begin.elapsed(),
        );
        Ok(self.need_compaction())
    }

    async fn try_commit(&self, table: Arc<dyn Table>) -> Result<()> {
        if !self.need_compaction() {
            // defensive checking
            return Ok(());
        }

        let ctx = &self.ctx;
        let base_snapshot = &self.base_snapshot;
        let mut table = table;
        let mut latest_snapshot = self.base_snapshot.clone();
        let mut retries = 0;
        let mut current_table_info = table.get_table_info();

        // newly appended segments of concurrent txs which are committed before us
        // the init value of it is an empty slice
        let mut concurrently_appended_segments_locations: &[Location] = &[];

        let fuse_segment_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        loop {
            let concurrently_append_segments_infos = fuse_segment_io
                .read_segments(concurrently_appended_segments_locations)
                .await?
                .into_iter()
                .collect::<Result<Vec<Arc<SegmentInfo>>>>()?;

            let concurrently_append_segments_stats = concurrently_append_segments_infos
                .iter()
                .map(|seg| &seg.summary)
                .collect::<Vec<_>>();

            // merge all the statistics
            let stats_concurrently_appended =
                reduce_statistics(&concurrently_append_segments_stats)?;

            let stats_merged_with_compacted = merge_statistics(
                &stats_concurrently_appended,
                &self.compacted_segment_accumulator.summary,
            )?;

            let stats_merged_with_unchanged = merge_statistics(
                &stats_merged_with_compacted,
                &self.unchanged_segment_accumulator.summary,
            )?;

            // chain all the locations, places
            // - the merged, potentially not compacted, concurrently appended segments at head
            // - the unchanged, compacted segments in the middle
            // - the compacted segments at the tail
            let locations = concurrently_appended_segments_locations
                .iter()
                .chain(self.unchanged_segment_accumulator.locations.iter())
                .chain(self.compacted_segment_accumulator.locations.iter());

            let mut snapshot_tobe_committed =
                TableSnapshot::from_previous(latest_snapshot.as_ref());
            snapshot_tobe_committed.segments = locations.into_iter().cloned().collect::<Vec<_>>();
            snapshot_tobe_committed.summary = stats_merged_with_unchanged;

            match FuseTable::commit_to_meta_server(
                ctx.as_ref(),
                current_table_info,
                &self.location_generator,
                snapshot_tobe_committed,
                &self.data_accessor,
            )
            .await
            {
                Err(e) => {
                    if e.code() == ErrorCode::table_version_mismatched_code() {
                        // refresh the table
                        table = table.refresh(ctx.as_ref()).await?;
                        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                        latest_snapshot =
                            fuse_table.read_table_snapshot(ctx.clone()).await?.ok_or_else(|| {
                              ErrorCode::LogicalError("compacting meets empty snapshot during conflict reconciliation")
                            })?;
                        current_table_info = &fuse_table.table_info;

                        // check conflicts between the base and latest snapshots
                        match MutatorConflictDetector::detect_conflicts(
                            base_snapshot,
                            &latest_snapshot,
                        ) {
                            Conflict::Unresolvable => {
                                counter!("fuse_compact_segments_unresolvable_conflict", 1);
                                counter!("fuse_compact_segments_aborts", 1);
                                abort_segment_compaction(
                                    self.ctx.clone(),
                                    self.data_accessor.clone(),
                                    &self.compacted_segment_accumulator.locations,
                                )
                                .await;
                                break Err(ErrorCode::StorageOther(
                                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                                ));
                            }
                            Conflict::ResolvableAppend(r) => {
                                counter!("fuse_compact_segments_resolvable_conflict", 1);
                                info!("resolvable conflicts detected");
                                concurrently_appended_segments_locations =
                                    &latest_snapshot.segments[r];
                            }
                        }

                        retries += 1;

                        counter!("fuse_compact_segments_retires", retries);
                        if retries >= MAX_RETRIES {
                            counter!("fuse_compact_segments_aborts", 1);
                            abort_segment_compaction(
                                self.ctx.clone(),
                                self.data_accessor.clone(),
                                &self.compacted_segment_accumulator.locations,
                            )
                            .await;
                            return Err(ErrorCode::StorageOther(format!(
                                "compact segment failed after {} retries",
                                retries
                            )));
                        }
                        // different from retry of commit_insert,  here operation retired
                        // without hesitation, to resolve the conflict as soon as possible
                        continue;
                    } else {
                        return Err(e);
                    }
                }
                Ok(_) => return Ok(()),
            }
        }
    }
}

async fn abort_segment_compaction(
    ctx: Arc<dyn TableContext>,
    operator: Operator,
    locations: &[Location],
) {
    let files = Files::create(ctx, operator);
    let paths = locations
        .iter()
        .map(|(path, _v)| path.clone())
        .collect::<Vec<_>>();
    files.remove_file_in_batch(&paths).await.unwrap_or_else(|e| {
        warn!(
            "failed to delete all the segment meta files while aborting segment compact operation. {} ",
            e
        )
    })
}
