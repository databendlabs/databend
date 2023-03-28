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
use common_exception::Result;
use metrics::gauge;
use opendal::Operator;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use tracing::info;

use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::CompactOptions;
use crate::statistics::reducers::merge_statistics_mut;
use crate::FuseTable;
use crate::TableContext;
use crate::TableMutator;

#[derive(Default)]
pub struct SegmentCompactionState {
    // summarised statistics of all the accumulated segments(compacted, and unchanged)
    pub statistics: Statistics,
    // locations of all the segments(compacted, and unchanged)
    pub segments_locations: Vec<Location>,
    // paths of all the newly created segments (which are compacted), need this to rollback the compaction
    pub new_segment_paths: Vec<String>,
    // number of fragmented segments compacted
    pub num_fragments_compacted: usize,
}

pub struct SegmentCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,
    compaction: SegmentCompactionState,
}

impl SegmentCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        compact_params: CompactOptions,
        location_generator: TableMetaLocationGenerator,
        operator: Operator,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            compact_params,
            data_accessor: operator,
            location_generator,
            compaction: Default::default(),
        })
    }

    fn has_compaction(&self) -> bool {
        !self.compaction.new_segment_paths.is_empty()
    }
}

#[async_trait::async_trait]
impl TableMutator for SegmentCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let select_begin = Instant::now();

        let mut base_segment_locations = self.compact_params.base_snapshot.segments.clone();
        if base_segment_locations.len() <= 1 {
            // no need to compact
            return Ok(false);
        }
        // traverse the segment in reversed order, so that newly created unmergeable fragmented segment
        // will be left at the "top", and likely to be merged in the next compaction; instead of leaving
        // an unmergeable fragmented segment in the middle.
        base_segment_locations.reverse();

        // need at lease 2 segments to make sense
        let num_segments = base_segment_locations.len();
        let limit = std::cmp::max(2, self.compact_params.limit.unwrap_or(num_segments));

        // prepare compactor
        let schema = Arc::new(self.compact_params.base_snapshot.schema.clone());
        let fuse_segment_io =
            SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone(), schema);
        let segment_writer = SegmentWriter::new(&self.data_accessor, &self.location_generator);
        let max_io_requests = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let compactor = SegmentCompactor::new(
            self.compact_params.block_per_seg as u64,
            max_io_requests,
            &fuse_segment_io,
            segment_writer,
        );

        self.compaction = compactor
            .compact(base_segment_locations, limit, |status| {
                self.ctx.set_status_info(&status);
            })
            .await?;

        gauge!(
            "fuse_compact_segments_select_duration_second",
            select_begin.elapsed(),
        );

        Ok(self.has_compaction())
    }

    async fn try_commit(self: Box<Self>, table: Arc<dyn Table>) -> Result<()> {
        if !self.has_compaction() {
            // defensive checking
            return Ok(());
        }

        let abort_action = AbortOperation {
            segments: self.compaction.new_segment_paths,
            ..Default::default()
        };

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .commit_mutation(
                &self.ctx,
                self.compact_params.base_snapshot,
                self.compaction.segments_locations,
                self.compaction.statistics,
                abort_action,
            )
            .await
    }
}

// Segments compactor that preserver the order of ingestion.
//
// Since the order of segments( and the order of blocks as well) should be preserved,
// if only segments of size "threshold" are allowed to be generated during compaction,
// there might be cases that to compact one fragmented segment, a large amount of
// non-fragmented segments have to be split into pieces and re-compacted.
//
// To avoid this "ripple effects", consecutive segments are allowed to be compacted into
// a new segment, if the size of compacted segment is lesser than 2 * threshold (exclusive).

pub struct SegmentCompactor<'a> {
    // Size of compacted segment should be in range R == [threshold, 2 * threshold)
    // within R, smaller one is preferred
    threshold: u64,
    // fragmented segment collected so far, it will be reset to empty if compaction occurs
    fragmented_segments: Vec<(Arc<SegmentInfo>, Location)>,
    // state which keep the number of blocks of all the fragmented segment collected so far,
    // it will be reset to 0 if compaction occurs
    accumulated_num_blocks: u64,
    chunk_size: usize,
    segment_reader: &'a SegmentsIO,
    segment_writer: SegmentWriter<'a>,
    // accumulated compaction state
    compacted_state: SegmentCompactionState,
}

impl<'a> SegmentCompactor<'a> {
    pub fn new(
        threshold: u64,
        chunk_size: usize,
        segment_reader: &'a SegmentsIO,
        segment_writer: SegmentWriter<'a>,
    ) -> Self {
        Self {
            threshold,
            accumulated_num_blocks: 0,
            fragmented_segments: vec![],
            chunk_size,
            segment_reader,
            segment_writer,
            compacted_state: Default::default(),
        }
    }

    pub async fn compact<T>(
        mut self,
        reverse_locations: Vec<Location>,
        limit: usize,
        status_callback: T,
    ) -> Result<SegmentCompactionState>
    where
        T: Fn(String),
    {
        let start = Instant::now();
        let number_segments = reverse_locations.len();
        // 1. feed segments into accumulator, taking limit into account
        let segments_io = self.segment_reader;
        let chunk_size = self.chunk_size;
        let mut checked_end_at = 0;
        let mut is_end = false;
        for chunk in reverse_locations.chunks(chunk_size) {
            let segment_infos = segments_io
                .read_segments(chunk, false)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;

            for (segment, location) in segment_infos.into_iter().zip(chunk.iter()) {
                self.add(segment, location.clone()).await?;
                let compacted = self.num_fragments_compacted();
                checked_end_at += 1;
                if compacted >= limit {
                    is_end = true;
                    // break if number of compacted segments reach the limit
                    // note that during the finalization of compaction, there might be some extra
                    // fragmented segments also need to be compacted, we just let it go
                    break;
                }
            }

            // Status.
            {
                let status = format!(
                    "compact segment: read segment files:{}/{}, cost:{} sec",
                    checked_end_at,
                    number_segments,
                    start.elapsed().as_secs()
                );
                info!(status);
                (status_callback)(status);
            }

            if is_end {
                break;
            }
        }
        let mut compaction = self.finalize().await?;

        // 2. combine with the unprocessed segments (which are outside of the limit)
        let fragments_compacted = !compaction.new_segment_paths.is_empty();
        if fragments_compacted {
            // if some compaction occurred, the reminders
            // which are outside of the limit should also be collected
            let start_pos = checked_end_at;
            for chunk in reverse_locations[start_pos..].chunks(chunk_size) {
                let segment_infos = segments_io
                    .read_segments(chunk, false)
                    .await?
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

                for (segment, location) in segment_infos.into_iter().zip(chunk.iter()) {
                    compaction.segments_locations.push(location.clone());
                    merge_statistics_mut(&mut compaction.statistics, &segment.summary)?;
                }

                checked_end_at += chunk.len();
                // Status.
                {
                    let status = format!(
                        "compact segment: read segment files:{}/{}, cost:{} sec",
                        checked_end_at,
                        number_segments,
                        start.elapsed().as_secs()
                    );
                    info!(status);
                    (status_callback)(status);
                }
            }
        }
        // reverse the segments back
        compaction.segments_locations.reverse();

        Ok(compaction)
    }

    // accumulate one segment
    pub async fn add(&mut self, segment_info: Arc<SegmentInfo>, location: Location) -> Result<()> {
        let num_blocks_current_segment = segment_info.blocks.len() as u64;

        if num_blocks_current_segment == 0 {
            return Ok(());
        }

        let s = self.accumulated_num_blocks + num_blocks_current_segment;

        if s < self.threshold {
            // not enough blocks yet, just keep this segment for later compaction
            self.accumulated_num_blocks = s;
            self.fragmented_segments.push((segment_info, location));
        } else if s >= self.threshold && s < 2 * self.threshold {
            // compact the fragmented segments
            self.fragmented_segments.push((segment_info, location));
            self.compact_fragments().await?;
        } else {
            // no choice but to compact the fragmented segments collected so far.
            // in this situation, after compaction, the size of compacted segments may be
            // lesser than threshold. this happens if the size of segment BEFORE compaction
            // is already larger than threshold.
            self.compact_fragments().await?;
            // after compaction the fragments, keep this segment as it is
            merge_statistics_mut(&mut self.compacted_state.statistics, &segment_info.summary)?;
            self.compacted_state.segments_locations.push(location);
        }

        Ok(())
    }

    async fn compact_fragments(&mut self) -> Result<()> {
        if self.fragmented_segments.is_empty() {
            return Ok(());
        }

        // 1. take the fragments and reset
        let fragments = std::mem::take(&mut self.fragmented_segments);
        self.accumulated_num_blocks = 0;

        // check if only one fragment left
        if fragments.len() == 1 {
            // if only one segment there, keep it as it is
            let (segment, location) = &fragments[0];
            merge_statistics_mut(&mut self.compacted_state.statistics, &segment.summary)?;
            self.compacted_state
                .segments_locations
                .push(location.clone());
            return Ok(());
        }

        // 2. build (and write down the compacted segment
        // 2.1 merge fragmented segments into new segment, and update the statistics
        let mut blocks = Vec::with_capacity(self.threshold as usize);
        let mut new_statistics = Statistics::default();

        self.compacted_state.num_fragments_compacted += fragments.len();
        for (segment, _location) in fragments {
            merge_statistics_mut(&mut new_statistics, &segment.summary)?;
            blocks.append(&mut segment.blocks.clone());
        }

        merge_statistics_mut(&mut self.compacted_state.statistics, &new_statistics)?;

        // 2.2 write down new segment
        let new_segment = SegmentInfo::new(blocks, new_statistics);
        let location = self.segment_writer.write_segment(new_segment).await?;
        self.compacted_state
            .new_segment_paths
            .push(location.0.clone());
        self.compacted_state.segments_locations.push(location);
        Ok(())
    }

    // return the number of compacted segments so far
    pub fn num_fragments_compacted(&self) -> usize {
        self.compacted_state.num_fragments_compacted
    }

    // finalize the compaction, compacts left fragments (if any)
    pub async fn finalize(mut self) -> Result<SegmentCompactionState> {
        if !self.fragmented_segments.is_empty() {
            // some fragments left, compact them
            self.compact_fragments().await?;
        }
        Ok(self.compacted_state)
    }
}
