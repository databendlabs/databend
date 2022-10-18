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

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_meta_app::schema::TableInfo;
use metrics::counter;
use metrics::gauge;
use opendal::Operator;
use tracing::info;
use tracing::warn;

use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::reducers::reduce_statistics;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;

const MAX_RETRIES: u64 = 10;
pub struct CompactSegmentMutator {
    ctx: Arc<dyn TableContext>,
    // the snapshot that compactor working on, it never changed during phases compaction.
    base_snapshot: Arc<TableSnapshot>,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,
    blocks_per_seg: usize,
    // chosen form the base_snapshot.segments which contains number of
    // blocks lesser than `blocks_per_seg`, and then compacted them into `compacted_segments`,
    // such that, at most one of them contains less than `blocks_per_seg` number of blocks,
    // and each of the others(if any) contains `blocks_per_seg` number of blocks.
    compacted_segment_accumulator: SegmentAccumualtor,
    // segments of base_snapshot that need not to be compacted, but should be included in
    // the new snapshot
    unchanged_segment_accumulator: SegmentAccumualtor,
    // segments_unchanged: Vec<(Location, Arc<SegmentInfo>)>,
}

struct SegmentAccumualtor {
    // location of accumulated segments
    location: Vec<Location>,
    // summarised statistics of all the accumulated segment
    summary: Statistics,
}

impl SegmentAccumualtor {
    fn new() -> Self {
        Self {
            location: vec![],
            summary: Default::default(),
        }
    }

    fn merge_stats(&mut self, stats: &Statistics) -> Result<()> {
        // TODO merge_inplace?
        self.summary = merge_statistics(&stats, &self.summary)?;
        Ok(())
    }

    fn add_location(&mut self, location: Location) {
        self.location.push(location)
    }

    fn is_empty(&self) -> bool {
        self.location.is_empty()
    }
}

impl CompactSegmentMutator {
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
            compacted_segment_accumulator: SegmentAccumualtor::new(),
            unchanged_segment_accumulator: SegmentAccumualtor::new(),
        })
    }
}

// The rough idea goes like this:
// 1. `target_select` will working and the base_snapshot: compact segments and keep the compactions
// 2. `try_commit` will try to commit the compacted snapshot to the meta store.
//     in condition of reconcilable conflicts detected, compactor will try to resolve the conflict and try again.
//     after MAX_RETRIES attempts, if still failed, abort the operation even if conflict resolvable.
//
// compaction starts from the head of the snapshot's segment vector.
// newly compacted segments will be added the end of snapshot's segment list, so that later
// when compaction of segments is executed incrementally, the newly added segments will be
// checked first.

#[async_trait::async_trait]
impl TableMutator for CompactSegmentMutator {
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

        let mut segments_tobe_compacted = vec![];
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

        // build new segments which are compacted. the newly compacted segments will also
        // be persistent into storage.
        for chunk in chunk_of_blocks {
            let stats = reduce_block_metas(chunk)?;
            self.compacted_segment_accumulator.merge_stats(&stats)?;
            let blocks: Vec<BlockMeta> = chunk.iter().map(|block| Clone::clone(*block)).collect();
            let new_segment = SegmentInfo::new(blocks, stats);
            let loc = segment_writer.write_segment(new_segment).await?;
            self.compacted_segment_accumulator.add_location(loc)
        }

        // keep the compacted_segments
        gauge!(
            "fuse_compact_segments_select_duration_second",
            select_begin.elapsed(),
        );

        Ok(!self.compacted_segment_accumulator.is_empty())
    }

    async fn try_commit(&self, table_info: &TableInfo) -> Result<()> {
        if self.compacted_segment_accumulator.is_empty() {
            return Ok(());
        }

        let ctx = &self.ctx;
        let base_snapshot = &self.base_snapshot;
        let catalog = ctx.get_catalog(table_info.catalog())?;
        let mut table = catalog.get_table_by_info(table_info)?;
        let mut latest_snapshot;
        let mut retries = 0;
        let mut current_table_info = table_info;

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

        // newly appended segments of concurrent txs which are committed before us
        // the init value of it is an empty slice
        let mut appended_segments_locations: &[Location] = &[];

        let fuse_segment_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        loop {
            let append_segments_infos = fuse_segment_io
                .read_segments(appended_segments_locations)
                .await?
                .into_iter()
                .collect::<Result<Vec<Arc<SegmentInfo>>>>()?;

            let append_segments_stats = append_segments_infos
                .iter()
                .map(|seg| &seg.summary)
                .collect::<Vec<_>>();

            // merge all the statistics
            let append_segments_merged_stats = reduce_statistics(&append_segments_stats)?;

            let merged_stats_with_compacted = merge_statistics(
                &append_segments_merged_stats,
                &self.compacted_segment_accumulator.summary,
            )?;

            let merged_stats_with_unchanged = merge_statistics(
                &merged_stats_with_compacted,
                &self.unchanged_segment_accumulator.summary,
            )?;

            // chain all the locations
            let locations = appended_segments_locations
                .iter()
                .chain(self.unchanged_segment_accumulator.location.iter())
                .chain(self.compacted_segment_accumulator.location.iter());

            let mut snapshot_tobe_committed = TableSnapshot::from_previous(base_snapshot.as_ref());
            snapshot_tobe_committed.segments = locations.into_iter().cloned().collect::<Vec<_>>();
            snapshot_tobe_committed.summary = merged_stats_with_unchanged;

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
                        table = table.refresh(ctx.as_ref()).await?;
                        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                        latest_snapshot =
                            fuse_table.read_table_snapshot(ctx.clone()).await?.ok_or_else(|| {
                              ErrorCode::LogicalError("compacting meets empty snapshot during conflict reconciliation")
                            })?;
                        current_table_info = &fuse_table.table_info;

                        match detect_conflicts(base_snapshot, &latest_snapshot) {
                            Conflict::Irreconcilable => {
                                counter!("fuse_compact_segments_irreconcilable_conflict", 1);
                                counter!("fuse_compact_segments_aborts", 1);
                                abort_segment_compaction(
                                    &self.data_accessor,
                                    &self.compacted_segment_accumulator.location,
                                )
                                .await;
                                break Err(ErrorCode::StorageOther(
                                    "mutation conflicts, concurrent mutation detected while committing segment compaction operation",
                                ));
                            }
                            Conflict::ReconcilableAppend(r) => {
                                counter!("fuse_compact_segments_reconcilable_conflict", 1);
                                info!("resolvable conflicts detected");
                                appended_segments_locations = &latest_snapshot.segments[r];
                            }
                        }

                        retries += 1;

                        counter!("fuse_compact_segments_retires", retries);
                        if retries >= MAX_RETRIES {
                            counter!("fuse_compact_segments_aborts", 1);
                            abort_segment_compaction(
                                &self.data_accessor,
                                &self.compacted_segment_accumulator.location,
                            )
                            .await;
                            return Err(ErrorCode::StorageOther(format!(
                                "compact segment failed after {} retries",
                                retries
                            )));
                        }
                        // different for retry of commit_insert, here operation retired without hesitation
                        // since... we are in a hurry ;)
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

enum Conflict {
    Irreconcilable,
    // reconcilable conflicts with append only option
    // the range embedded is the range of segments that are appended in the latest snapshot
    ReconcilableAppend(Range<usize>),
}

fn detect_conflicts(base: &TableSnapshot, latest: &TableSnapshot) -> Conflict {
    let base_segments = &base.segments;
    let latest_segments = &latest.segments;

    let base_segments_len = base_segments.len();
    let latest_segments_len = latest_segments.len();

    if latest_segments_len >= base_segments_len
        && base_segments[base_segments_len - 1] == latest_segments[latest_segments_len - 1]
        && base_segments[0] == latest_segments[latest_segments_len - base_segments_len]
    {
        Conflict::ReconcilableAppend(0..(latest_segments_len - base_segments_len))
    } else {
        Conflict::Irreconcilable
    }
}

async fn abort_segment_compaction(data_accessor: &Operator, locations: &[Location]) {
    // parallel delete?
    for (path, _) in locations {
        let object = data_accessor.object(path.as_str());
        object.delete().await.unwrap_or_else(|e| {
            warn!(
                "failed to delete object [{}] while aborting segment compact operation. {} ",
                path, e
            )
        });
    }
}
