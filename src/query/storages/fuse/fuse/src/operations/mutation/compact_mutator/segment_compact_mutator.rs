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
use common_storages_fuse_meta::caches::CacheManager;
use common_storages_fuse_meta::meta::Location;
use common_storages_fuse_meta::meta::SegmentInfo;
use common_storages_fuse_meta::meta::Statistics;
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

pub struct SegmentCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,

    // summarised statistics of all the accumulated segments(compacted, and unchanged)
    merged_segment_statistics: Statistics,
    // locations all the accumulated segments(compacted, and unchanged)
    merged_segments_locations: Vec<Location>,
    // paths all the newly created segments (which are compacted)
    new_segment_paths: Vec<String>,
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
            merged_segment_statistics: Statistics::default(),
            merged_segments_locations: vec![],
            new_segment_paths: vec![],
        })
    }

    fn need_compaction(&self) -> bool {
        !self.new_segment_paths.is_empty()
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

        let mut accumulator =
            SegmentAccumulator::new(self.compact_params.block_per_seg as u64, segment_writer);

        // feed segments into accumulator
        let mut compacted = 0;
        for (idx, x) in base_segments.iter().enumerate() {
            accumulator
                .add(x, base_segment_locations[idx].clone())
                .await?;
            compacted += 1;
            if compacted >= limit {
                break;
            }
        }
        accumulator.finalize().await?;

        // gather the results
        self.new_segment_paths = accumulator.new_segments;
        self.merged_segment_statistics = accumulator.merged_statistics;
        self.merged_segments_locations = accumulator.segment_locations;

        gauge!(
            "fuse_compact_segments_select_duration_second",
            select_begin.elapsed(),
        );
        Ok(self.need_compaction())
    }

    async fn try_commit(self: Box<Self>, table: Arc<dyn Table>) -> Result<()> {
        if !self.need_compaction() {
            // defensive checking
            return Ok(());
        }

        let abort_action = AbortOperation {
            segments: self.new_segment_paths,
            ..Default::default()
        };

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .commit_mutation(
                &self.ctx,
                self.compact_params.base_snapshot,
                self.merged_segments_locations,
                self.merged_segment_statistics,
                abort_action,
            )
            .await
    }
}

pub struct SegmentAccumulator<'a> {
    // summary statistics of compacted segments (merged and unchanged segments)
    pub merged_statistics: Statistics,
    // locations of compacted segments (merged and unchanged segments)
    pub segment_locations: Vec<Location>,
    // locations of newly generated segments, need this do undo the operation
    pub new_segments: Vec<String>,
    threshold: u64,
    accumulated_num_blocks: u64,
    fragmented_segments: Vec<(&'a SegmentInfo, Location)>,
    segment_writer: SegmentWriter<'a>,
}

impl<'a> SegmentAccumulator<'a> {
    pub fn new(threshold: u64, segment_writer: SegmentWriter<'a>) -> Self {
        Self {
            threshold,
            accumulated_num_blocks: 0,
            merged_statistics: Statistics::default(),
            segment_locations: vec![],
            fragmented_segments: vec![],
            new_segments: vec![],
            segment_writer,
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
                // not enough blocks yet, just keep this segment
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
        // TODO doc this
        self.barrier_compact_with(segment_info, location).await?;

        Ok(())
    }

    async fn compact_fragments(&mut self) -> Result<()> {
        // 1. take the fragments and reset
        let fragments = std::mem::take(&mut self.fragmented_segments);
        self.accumulated_num_blocks = 0;

        // 2. build and write down the compacted segment

        // check if only one fragment left

        if fragments.len() == 1 {
            let (segment, location) = &fragments[0];
            merge_statistics_mut(&mut self.merged_statistics, &segment.summary)?;
            self.segment_locations.push(location.clone());
            return Ok(());
        }

        // 2.1 build new segment and merge statistics
        let mut blocks = Vec::with_capacity(self.threshold as usize);
        let mut new_statistics = Statistics::default();
        for (segment, _location) in fragments {
            merge_statistics_mut(&mut new_statistics, &segment.summary)?;
            blocks.append(&mut segment.blocks.clone());
        }
        merge_statistics_mut(&mut self.merged_statistics, &new_statistics)?;

        // 2.2 write down new segment
        let new_segment = SegmentInfo::new(blocks, new_statistics);
        let location = self.segment_writer.write_segment(new_segment).await?;
        self.new_segments.push(location.0.clone());
        self.segment_locations.push(location);
        Ok(())
    }

    async fn barrier_compact_with(
        &mut self,
        segment_info: &'a SegmentInfo,
        location: Location,
    ) -> Result<()> {
        let num_block = segment_info.blocks.len() as u64;
        if self.accumulated_num_blocks + num_block < 2 * self.threshold {
            // TODO doc why we need this branch
            merge_statistics_mut(&mut self.merged_statistics, &segment_info.summary)?;
            self.segment_locations.push(location);
            self.compact_fragments().await?;
        } else {
            // we have no choice but to compact the fragmented segments collected so far,
            // in this situation, after compaction, the size compacted segment may NOT
            // be of perfect size (smaller, but not larger).
            // this happens if the size of segment BEFORE compaction is already
            // larger than threshold.
            self.compact_fragments().await?;
            // after compaction, we keep this segment directly as compacted, since
            // it is large enough
            merge_statistics_mut(&mut self.merged_statistics, &segment_info.summary)?;
            self.segment_locations.push(location);
        }

        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<()> {
        if !self.fragmented_segments.is_empty() {
            // some fragments left, compact them with the last compacted segment
            self.compact_fragments().await?;
        }
        Ok(())
    }
}
