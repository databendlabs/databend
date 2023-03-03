// Copyright 2023 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::vec;

use super::compact_part::CompactTask;
use common_catalog::plan::Partitions;
use common_exception::Result;
use common_expression::BlockThresholds;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;

use super::compact_part::CompactPartInfo;
use crate::io::SegmentsIO;
use crate::operations::CompactOptions;
use crate::statistics::reducers::merge_statistics_mut;
use crate::TableContext;

#[derive(Clone)]
pub struct BlockCompactMutator {
    pub ctx: Arc<dyn TableContext>,

    pub compact_params: CompactOptions,
    pub operator: Operator,
    // The order of the unchanged segments in snapshot.
    pub unchanged_segment_indices: Vec<usize>,
    // locations all the unchanged segments.
    pub unchanged_segment_locations: Vec<Location>,
    // summarised statistics of all the unchanged segments
    pub unchanged_segment_statistics: Statistics,
}

struct CompactSegmentTask {
    order: usize,
    task: Vec<Arc<SegmentInfo>>,
}

impl BlockCompactMutator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        compact_params: CompactOptions,
        operator: Operator,
    ) -> Self {
        Self {
            ctx,
            compact_params,
            operator,
            unchanged_segment_indices: Vec::new(),
            unchanged_segment_locations: Vec::new(),
            unchanged_segment_statistics: Statistics::default(),
        }
    }

    pub async fn target_select(&mut self) -> Result<bool> {
        let snapshot = self.compact_params.base_snapshot.clone();
        let segment_locations = &snapshot.segments;

        let schema = Arc::new(self.compact_params.base_snapshot.schema.clone());
        // Read all segments information in parallel.
        let segments_io = SegmentsIO::create(self.ctx.clone(), self.operator.clone(), schema);
        let segment_infos = segments_io
            .read_segments(segment_locations)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let segment_tasks = self.gen_segment_tasks(&segment_infos, segment_locations)?;
        if segment_tasks.is_empty() {
            return Ok(false);
        }

        Ok(true)
    }

    fn gen_segment_tasks(&mut self, segment_infos: &Vec<Arc<SegmentInfo>>, segment_locations: &Vec<Location>) -> Result<Vec<CompactSegmentTask>>{
        let number_segments = segment_infos.len();
        let limit = self.compact_params.limit.unwrap_or(number_segments);
        let mut order = 0;
        let mut end = 0;
        let mut compacted_segment_cnt = 0;

        let mut builder = SegmentTaskBuilder::new(self.compact_params.block_per_seg as u64);
        let mut segment_tasks = Vec::new();

        for (idx, segment) in segment_infos.iter().enumerate() {
            let tasks = builder.add(segment.clone());
            for task in tasks {
                if SegmentTaskBuilder::check_for_compact(&task) {
                    compacted_segment_cnt += task.len();
                    segment_tasks.push(CompactSegmentTask {
                        order,
                        task,
                    });
                } else {
                    self.unchanged_segment_locations
                        .push(segment_locations[idx].clone());
                    self.unchanged_segment_indices.push(order);
                    merge_statistics_mut(
                        &mut self.unchanged_segment_statistics,
                        &segment_infos[idx].summary,
                    )?;
                }
                order += 1;
            }
            end = idx + 1;
            if compacted_segment_cnt + builder.segments.len() >= limit {
                break;
            }
        }

        if !builder.segments.is_empty() {
            let task = std::mem::take(&mut builder.segments);
            if SegmentTaskBuilder::check_for_compact(&task) {
                segment_tasks.push(CompactSegmentTask {
                    order,
                    task,
                });
            } else {
                self.unchanged_segment_locations
                    .push(segment_locations[end - 1].clone());
                self.unchanged_segment_indices.push(order);
                merge_statistics_mut(
                    &mut self.unchanged_segment_statistics,
                    &segment_infos[end - 1].summary,
                )?;
            }
            order += 1;
        }

        if end < number_segments {
            for i in end..number_segments {
                self.unchanged_segment_locations
                    .push(segment_locations[i].clone());
                self.unchanged_segment_indices.push(order);
                merge_statistics_mut(&mut self.unchanged_segment_statistics, &segment_infos[i].summary)?;
                order += 1;
            }
        }
        Ok(segment_tasks)
    }

    fn gen_block_tasks() {

    }
}

#[derive(Default)]
struct SegmentTaskBuilder {
    segments: Vec<Arc<SegmentInfo>>,
    block_count: u64,
    threshold: u64,
}

impl SegmentTaskBuilder {
    fn new(threshold: u64) -> Self {
        Self {
            threshold,
            ..Default::default()
        }
    }

    fn check_for_compact(segments: &Vec<Arc<SegmentInfo>>) -> bool {
        segments.len() != 1
            || (segments[0].summary.block_count > 1
                && segments[0].summary.perfect_block_count != segments[0].summary.block_count)
    }

    fn add(&mut self, segment: Arc<SegmentInfo>) -> Vec<Vec<Arc<SegmentInfo>>> {
        self.block_count += segment.summary.block_count;
        if self.block_count < self.threshold {
            self.segments.push(segment);
            return vec![];
        }

        if self.block_count > 2 * self.threshold {
            self.block_count = 0;
            let trivial = vec![segment];
            if self.segments.is_empty() {
                return vec![trivial];
            } else {
                return vec![std::mem::take(&mut self.segments), trivial];
            }
        }

        self.block_count = 0;
        self.segments.push(segment);
        vec![std::mem::take(&mut self.segments)]
    }
}


#[derive(Default)]
struct BlockTaskBuilder {
    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
}

impl BlockTaskBuilder {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn add(&mut self, block: &Arc<BlockMeta>, thresholds: BlockThresholds) -> Vec<CompactTask> {
        self.total_rows += block.row_count as usize;
        self.total_size += block.block_size as usize;

        if !thresholds.check_large_enough(self.total_rows, self.total_size) {
            // blocks < N
            self.blocks.push(block.clone());
            return vec![];
        }

        let tasks = if !thresholds.check_for_compact(self.total_rows, self.total_size) {
            // blocks > 2N
            let trivial_task = CompactTask::Trivial(block.clone());
            if !self.blocks.is_empty() {
                let compact_task = Self::create_task(std::mem::take(&mut self.blocks));
                vec![compact_task, trivial_task]
            } else {
                vec![trivial_task]
            }
        } else {
            // N <= blocks < 2N
            self.blocks.push(block.clone());
            vec![Self::create_task(std::mem::take(&mut self.blocks))]
        };

        self.total_rows = 0;
        self.total_size = 0;
        tasks
    }

    fn finalize(&mut self, task: Option<CompactTask>) -> CompactTask {
        let mut blocks = task.map_or(vec![], |t| t.get_block_metas());
        blocks.extend(std::mem::take(&mut self.blocks));
        Self::create_task(blocks)
    }

    fn create_task(blocks: Vec<Arc<BlockMeta>>) -> CompactTask {
        match blocks.len() {
            0 => panic!("the blocks is empty"),
            1 => CompactTask::Trivial(blocks[0].clone()),
            _ => CompactTask::Normal(blocks),
        }
    }
}
