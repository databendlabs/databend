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

use std::any::Any;
use std::sync::Arc;
use std::vec;

use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::Versioned;
use opendal::Operator;

use crate::io::BlockCompactor;
use crate::io::SegmentWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::metrics_set_segments_memory_usage;
use crate::metrics::metrics_set_selected_blocks_memory_usage;
use crate::operations::mutation::AbortOperation;
use crate::operations::AppendOperationLogEntry;
use crate::operations::CompactOptions;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::reduce_block_metas;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::TableMutator;

pub struct BlockCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    block_compactor: BlockCompactor,
    location_generator: TableMetaLocationGenerator,
    compact_tasks: Vec<Tasks>,
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
        let mut helper = TaskHelper::new(self.block_compactor.clone());

        let mut unchanged_segment_statistics = Statistics::default();

        let mut order = 0;
        let mut end = 0;
        let mut compacted_segment_cnt = 0;

        for (idx, segment) in segments.iter().enumerate() {
            let mut maybe = task.is_empty()
                && helper.blocks.is_empty()
                && segment.summary.block_count as usize == blocks_per_seg;
            for b in segment.blocks.iter() {
                let res = helper.add_block(b);
                if res.len() != 1 {
                    maybe = false;
                }
                task.extend(res);

                if task.len() == blocks_per_seg {
                    order += 1;
                    let one_task = Tasks::create(task.clone(), order);
                    self.compact_tasks.push(one_task);
                    task.clear();
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

        if !helper.is_empty() {
            let mut blocks = task.pop().map_or(vec![], |t| t.get_block_metas());
            blocks.extend(helper.blocks.clone());
            helper.blocks = blocks;
            task.push(helper.create_task());
        }

        if !task.is_empty() {
            order += 1;
            let one_task = Tasks::create(task.clone(), order);
            self.compact_tasks.push(one_task);
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

        match self.compact_tasks.len() {
            0 => Ok(false),
            1 if self.compact_tasks[0].is_all_rivial() => Ok(false),
            _ => Ok(true),
        }
    }
}

pub struct Tasks {
    task: Vec<Arc<dyn Task>>,
    order: usize,
}

impl Tasks {
    fn create(task: Vec<Arc<dyn Task>>, order: usize) -> Self {
        Self { task, order }
    }

    fn is_all_rivial(&self) -> bool {
        self.task
            .iter()
            .all(|v| v.as_any().downcast_ref::<TrivialTask>().is_some())
    }
}

struct TaskHelper {
    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
    block_compactor: BlockCompactor,
}

impl TaskHelper {
    fn new(block_compactor: BlockCompactor) -> Self {
        TaskHelper {
            blocks: vec![],
            total_rows: 0,
            total_size: 0,
            block_compactor,
        }
    }

    fn clear(&mut self) {
        self.blocks.clear();
        self.total_rows = 0;
        self.total_size = 0;
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn add_block(&mut self, block: &Arc<BlockMeta>) -> Vec<Arc<dyn Task>> {
        self.total_rows += block.row_count as usize;
        self.total_size += block.block_size as usize;

        if !self
            .block_compactor
            .check_perfect_block(self.total_rows, self.total_size)
        {
            self.blocks.push(block.clone());
            return vec![];
        }

        if !self
            .block_compactor
            .check_for_compact(self.total_rows, self.total_size)
        {
            let trival_task = TrivialTask::create(block.clone());
            let res = if !self.blocks.is_empty() {
                let compact_task = CompactTask::create(self.blocks.clone());
                vec![compact_task, trival_task]
            } else {
                vec![trival_task]
            };
            self.clear();
            return res;
        }

        self.blocks.push(block.clone());
        let compact_task = CompactTask::create(self.blocks.clone());
        self.clear();
        vec![compact_task]
    }

    fn create_task(&self) -> Arc<dyn Task> {
        match self.blocks.len() {
            0 => panic!("the blocks is empty"),
            1 => TrivialTask::create(self.blocks[0].clone()),
            _ => CompactTask::create(self.blocks.clone()),
        }
    }
}

pub trait Task {
    fn get_block_metas(&self) -> Vec<Arc<BlockMeta>>;

    fn as_any(&self) -> &dyn Any;
}

pub struct TrivialTask {
    block: Arc<BlockMeta>,
}

impl TrivialTask {
    fn create(block: Arc<BlockMeta>) -> Arc<dyn Task> {
        Arc::new(Self { block })
    }
}

impl Task for TrivialTask {
    fn get_block_metas(&self) -> Vec<Arc<BlockMeta>> {
        vec![self.block.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct CompactTask {
    blocks: Vec<Arc<BlockMeta>>,
}

impl CompactTask {
    fn create(blocks: Vec<Arc<BlockMeta>>) -> Arc<dyn Task> {
        Arc::new(Self { blocks })
    }
}

impl Task for CompactTask {
    fn get_block_metas(&self) -> Vec<Arc<BlockMeta>> {
        self.blocks.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
