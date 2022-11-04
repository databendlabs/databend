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
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use metrics::gauge;
use opendal::Operator;

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
    compacted: SegmentCompactionState,
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
            compacted: Default::default(),
        })
    }

    fn has_compaction(&self) -> bool {
        !self.compacted.new_segment_paths.is_empty()
    }
}

#[async_trait::async_trait]
impl TableMutator for SegmentCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let select_begin = Instant::now();

        // read all the segments
        let fuse_segment_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let base_segment_locations = &self.compact_params.base_snapshot.segments;
        let base_segments = fuse_segment_io
            .read_segments(&self.compact_params.base_snapshot.segments)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let number_segments = base_segments.len();
        let limit = self.compact_params.limit.unwrap_or(number_segments);

        // prepare accumulator
        let segment_info_cache = CacheManager::instance().get_table_segment_cache();
        let segment_writer = SegmentWriter::new(
            &self.data_accessor,
            &self.location_generator,
            &segment_info_cache,
        );

        let mut compactor =
            SegmentCompactor::new(self.compact_params.block_per_seg as u64, segment_writer);

        // feed segments into accumulator
        for (idx, x) in base_segments.iter().enumerate() {
            compactor
                .add(x, base_segment_locations[idx].clone())
                .await?;
            let compacted = compactor.current_compacted();
            if compacted >= limit {
                // break if number of compacted segments reach the limit
                // note that during the finalization of compaction, there might be some extra
                // fragmented segments also need to be compacted, we just let it go
                break;
            }
        }

        let compacted_state = compactor.finalize().await?;

        // gather the results
        self.compacted = compacted_state;

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
            segments: self.compacted.new_segment_paths,
            ..Default::default()
        };

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .commit_mutation(
                &self.ctx,
                self.compact_params.base_snapshot,
                self.compacted.segments_locations,
                self.compacted.statistics,
                abort_action,
            )
            .await
    }
}

pub struct SegmentCompactor<'a> {
    threshold: u64,
    accumulated_num_blocks: u64,
    fragmented_segments: Vec<(&'a SegmentInfo, Location)>,
    segment_writer: SegmentWriter<'a>,
    compacted_state: SegmentCompactionState,
}

impl<'a> SegmentCompactor<'a> {
    pub fn new(threshold: u64, segment_writer: SegmentWriter<'a>) -> Self {
        Self {
            threshold,
            accumulated_num_blocks: 0,
            fragmented_segments: vec![],
            segment_writer,
            compacted_state: Default::default(),
        }
    }

    // accumulate one segment
    pub async fn add(&mut self, segment_info: &'a SegmentInfo, location: Location) -> Result<()> {
        let num_blocks_current_segment = segment_info.blocks.len() as u64;

        if num_blocks_current_segment == 0 {
            return Ok(());
        }

        if num_blocks_current_segment <= self.threshold {
            let s = self.accumulated_num_blocks + num_blocks_current_segment;
            if s < self.threshold {
                // not enough blocks yet, just keep this segment for later compaction
                self.accumulated_num_blocks = s;
                self.fragmented_segments.push((segment_info, location));
                return Ok(());
            } else if s >= self.threshold && s < 2 * self.threshold {
                // compact the fragmented segments
                self.fragmented_segments.push((segment_info, location));
                self.compact_fragments().await?;
                return Ok(());
            }
        }
        // input segment is larger than threshold , try to
        // - either compact this large segment with previous collected fragmented segments
        //   if the combined size is lesser than 2 * threshold
        // - or compact previous collected fragmented segments only, and keep this large segment as it is
        self.barrier_compact_with(segment_info, location).await?;

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

    async fn barrier_compact_with(
        &mut self,
        segment_info: &'a SegmentInfo,
        location: Location,
    ) -> Result<()> {
        let num_block = segment_info.blocks.len() as u64;
        // check the segments collected so far
        if self.accumulated_num_blocks + num_block < 2 * self.threshold {
            // if the combined size is lesser than 2 * threshold, just compact them
            self.fragmented_segments.push((segment_info, location));
            self.compact_fragments().await?;
        } else {
            // no choice but to compact the fragmented segments collected so far,
            // in this situation, after compaction, the size of compacted segments may be
            // lesser than threshold.
            // this happens if the size of segment BEFORE compaction is already larger than threshold.
            self.compact_fragments().await?;
            // after compaction the fragments, keep this segment as it is
            merge_statistics_mut(&mut self.compacted_state.statistics, &segment_info.summary)?;
            self.compacted_state.segments_locations.push(location);
        }

        Ok(())
    }

    pub fn current_compacted(&self) -> usize {
        self.compacted_state.num_fragments_compacted
    }

    pub async fn finalize(mut self) -> Result<SegmentCompactionState> {
        if !self.fragmented_segments.is_empty() {
            // some fragments left, compact them
            self.compact_fragments().await?;
        }
        Ok(self.compacted_state)
    }
}
