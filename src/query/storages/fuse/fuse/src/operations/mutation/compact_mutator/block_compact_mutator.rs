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
use std::vec;

use common_catalog::plan::Partitions;
use common_datablocks::BlockCompactThresholds;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::Statistics;
use opendal::Operator;

use super::compact_part::CompactTask;
use super::CompactPartInfo;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_set_segments_memory_usage;
use crate::operations::CompactOptions;
use crate::statistics::merge_statistics;
use crate::TableContext;

pub struct BlockCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    thresholds: BlockCompactThresholds,
    location_generator: TableMetaLocationGenerator,
    compact_tasks: Partitions,
    // summarised statistics of all the unchanged segments
    unchanged_segment_statistics: Statistics,
    // locations all the unchanged segments, with index and location.
    unchanged_segments_locations: Vec<(usize, Location)>,
}

impl BlockCompactMutator {
    async fn target_select(&mut self) -> Result<bool> {
        let snapshot = self.compact_params.base_snapshot.clone();
        let segment_locations = &snapshot.segments;

        // Read all segments information in parallel.
        let segments_io = SegmentsIO::create(self.ctx.clone(), self.data_accessor.clone());
        let segments = segments_io
            .read_segments(segment_locations)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let number_segments = segments.len();

        // todo: add real metrics
        metrics_set_segments_memory_usage(0.0);

        let limit = self.compact_params.limit.unwrap_or(number_segments);
        let blocks_per_seg = self.compact_params.block_per_seg;

        let mut task = Vec::with_capacity(self.compact_params.block_per_seg);
        let mut builder = CompactTaskBuilder::default();

        let mut unchanged_segment_statistics = Statistics::default();

        let mut order = 0;
        let mut end = 0;
        let mut compacted_segment_cnt = 0;

        for (idx, segment) in segments.iter().enumerate() {
            let mut maybe = task.is_empty()
                && builder.is_empty()
                && segment.summary.block_count as usize == blocks_per_seg;
            for b in segment.blocks.iter() {
                let res = builder.add_block(b, self.thresholds);
                if res.len() != 1 {
                    maybe = false;
                }
                task.extend(res);

                if task.len() == blocks_per_seg {
                    order += 1;
                    self.compact_tasks
                        .push(CompactPartInfo::create(std::mem::take(&mut task), order));
                }
            }
            if maybe {
                self.compact_tasks.pop();
                self.unchanged_segments_locations
                    .push((order, segment_locations[idx].clone()));
                unchanged_segment_statistics =
                    merge_statistics(&unchanged_segment_statistics, &segments[idx].summary)?;
                continue;
            }
            compacted_segment_cnt += 1;
            if compacted_segment_cnt == limit {
                end = idx + 1;
                break;
            }
        }

        if !builder.is_empty() {
            let mut blocks = task.pop().map_or(vec![], |t| t.get_block_metas());
            blocks.extend(builder.blocks.clone());
            builder.blocks = blocks;
            task.push(builder.create_task());
        }

        if !task.is_empty() {
            order += 1;
            self.compact_tasks
                .push(CompactPartInfo::create(task, order));
        }

        if self.compact_tasks.is_empty()
            || (self.compact_tasks.len() == 1
                && CompactPartInfo::from_part(&self.compact_tasks[0])
                    .unwrap()
                    .is_all_trivial())
        {
            return Ok(false);
        }

        if end < number_segments {
            for i in end..number_segments {
                order += 1;
                self.unchanged_segments_locations
                    .push((order, segment_locations[i].clone()));
                unchanged_segment_statistics =
                    merge_statistics(&unchanged_segment_statistics, &segments[i].summary)?;
            }
        }

        Ok(true)
    }
}

#[derive(Default)]
struct CompactTaskBuilder {
    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
}

impl CompactTaskBuilder {
    fn clear(&mut self) {
        self.blocks.clear();
        self.total_rows = 0;
        self.total_size = 0;
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn add_block(
        &mut self,
        block: &Arc<BlockMeta>,
        thresholds: BlockCompactThresholds,
    ) -> Vec<CompactTask> {
        self.total_rows += block.row_count as usize;
        self.total_size += block.block_size as usize;

        if !thresholds.check_large_enough(self.total_rows, self.total_size) {
            self.blocks.push(block.clone());
            return vec![];
        }

        if !thresholds.check_for_compact(self.total_rows, self.total_size) {
            let trival_task = CompactTask::Trival(block.clone());
            let res = if !self.blocks.is_empty() {
                let compact_task = CompactTask::Normal(self.blocks.clone());
                vec![compact_task, trival_task]
            } else {
                vec![trival_task]
            };
            self.clear();
            return res;
        }

        self.blocks.push(block.clone());
        let compact_task = CompactTask::Normal(self.blocks.clone());
        self.clear();
        vec![compact_task]
    }

    fn create_task(&self) -> CompactTask {
        match self.blocks.len() {
            0 => panic!("the blocks is empty"),
            1 => CompactTask::Trival(self.blocks[0].clone()),
            _ => CompactTask::Normal(self.blocks.clone()),
        }
    }
}
