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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::vec;

use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::ColumnId;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;

use crate::io::SegmentsIO;
use crate::operations::merge_into::mutation_meta::mutation_log::BlockMetaIndex;
use crate::operations::mutation::CompactPartInfo;
use crate::operations::CompactOptions;
use crate::statistics::reducers::merge_statistics_mut;
use crate::TableContext;

#[derive(Clone)]
pub struct BlockCompactMutator {
    pub ctx: Arc<dyn TableContext>,
    pub operator: Operator,

    pub thresholds: BlockThresholds,
    pub compact_params: CompactOptions,
    pub column_ids: HashSet<ColumnId>,

    // A set of Parts.
    pub compact_tasks: Partitions,
    pub unchanged_blocks_map: HashMap<usize, BTreeMap<usize, Arc<BlockMeta>>>,
    // locations all the unchanged segments.
    pub unchanged_segments_map: BTreeMap<usize, Location>,
    // summarised statistics of all the unchanged segments
    pub unchanged_segment_statistics: Statistics,
}

impl BlockCompactMutator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        thresholds: BlockThresholds,
        compact_params: CompactOptions,
        column_ids: HashSet<ColumnId>,
        operator: Operator,
    ) -> Self {
        Self {
            ctx,
            operator,
            thresholds,
            compact_params,
            column_ids,
            unchanged_blocks_map: HashMap::new(),
            compact_tasks: Partitions::create_nolazy(PartitionsShuffleKind::Mod, vec![]),
            unchanged_segments_map: BTreeMap::new(),
            unchanged_segment_statistics: Statistics::default(),
        }
    }

    pub async fn target_select(&mut self) -> Result<()> {
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

        let number_segments = segment_infos.len();
        let limit = self.compact_params.limit.unwrap_or(number_segments);
        let mut end = 0;
        let mut segment_idx = 0;
        let mut compacted_segment_cnt = 0;

        let mut checker = SegmentCompactChecker::new(self.compact_params.block_per_seg as u64);
        for (idx, segment) in segment_infos.iter().enumerate() {
            let segments_vec = checker.add(segment.clone());
            for segments in segments_vec {
                if SegmentCompactChecker::check_for_compact(&segments) {
                    compacted_segment_cnt += segments.len();
                    self.build_compact_tasks(segments, segment_idx);
                } else {
                    self.unchanged_segments_map
                        .insert(segment_idx, segment_locations[idx].clone());
                    merge_statistics_mut(
                        &mut self.unchanged_segment_statistics,
                        &segment_infos[idx].summary,
                    )?;
                }
                segment_idx += 1;
            }
            end = idx + 1;
            if compacted_segment_cnt + checker.segments.len() >= limit {
                break;
            }
        }

        if !checker.segments.is_empty() {
            let segments = std::mem::take(&mut checker.segments);
            if SegmentCompactChecker::check_for_compact(&segments) {
                self.build_compact_tasks(segments, segment_idx);
            } else {
                self.unchanged_segments_map
                    .insert(segment_idx, segment_locations[end - 1].clone());
                merge_statistics_mut(
                    &mut self.unchanged_segment_statistics,
                    &segment_infos[end - 1].summary,
                )?;
            }
            segment_idx += 1;
        }

        if end < number_segments {
            for i in end..number_segments {
                self.unchanged_segments_map
                    .insert(segment_idx, segment_locations[i].clone());
                merge_statistics_mut(
                    &mut self.unchanged_segment_statistics,
                    &segment_infos[i].summary,
                )?;
                segment_idx += 1;
            }
        }
        Ok(())
    }

    // Select the row_count >= min_rows_per_block or block_size >= max_bytes_per_block
    // as the perfect_block condition(N for short). Gets a set of segments, iterates
    // through the blocks, and finds the blocks >= N and blocks < 2N as a task.
    fn build_compact_tasks(&mut self, segments: Vec<Arc<SegmentInfo>>, segment_idx: usize) {
        let mut builder = CompactTaskBuilder::new(self.column_ids.clone());
        let mut tasks = VecDeque::new();
        let mut block_idx = 0;
        let mut unchanged_blocks = BTreeMap::new();
        // The order of the compact is from old to new.
        for segment in segments.iter().rev() {
            for block in segment.blocks.iter() {
                let (unchanged, need_take) = builder.add(block, self.thresholds);
                if need_take {
                    let blocks = builder.take_blocks();
                    if blocks.len() == 1 && builder.check_column_ids(&blocks[0]) {
                        unchanged_blocks.insert(block_idx, blocks[0].clone());
                    } else {
                        tasks.push_back((block_idx, blocks));
                    }
                    block_idx += 1;
                }
                if unchanged {
                    unchanged_blocks.insert(block_idx, block.clone());
                    block_idx += 1;
                }
            }
        }

        if !builder.is_empty() {
            let (index, mut blocks) = tasks.pop_back().unwrap_or(
                unchanged_blocks
                    .pop_last()
                    .map_or((0, vec![]), |(k, v)| (k, vec![v])),
            );
            blocks.extend(builder.take_blocks());
            if blocks.len() > 1 || builder.check_column_ids(&blocks[0]) {
                tasks.push_back((index, blocks));
            }
        }

        let mut partitions = tasks
            .into_iter()
            .map(|(block_idx, blocks)| {
                CompactPartInfo::create(blocks, BlockMetaIndex {
                    segment_idx,
                    block_idx,
                    range: None,
                })
            })
            .collect();
        self.compact_tasks.partitions.append(&mut partitions);
        self.unchanged_blocks_map
            .insert(segment_idx, unchanged_blocks);
    }
}

struct SegmentCompactChecker {
    segments: Vec<Arc<SegmentInfo>>,
    total_block_count: u64,
    threshold: u64,
}

impl SegmentCompactChecker {
    fn new(threshold: u64) -> Self {
        Self {
            threshold,
            total_block_count: 0,
            segments: vec![],
        }
    }

    fn check_for_compact(segments: &Vec<Arc<SegmentInfo>>) -> bool {
        segments.len() != 1
            || (segments[0].summary.block_count > 1
                && segments[0].summary.perfect_block_count != segments[0].summary.block_count)
    }

    fn add(&mut self, segment: Arc<SegmentInfo>) -> Vec<Vec<Arc<SegmentInfo>>> {
        self.total_block_count += segment.summary.block_count;
        if self.total_block_count < self.threshold {
            self.segments.push(segment);
            return vec![];
        }

        if self.total_block_count > 2 * self.threshold {
            self.total_block_count = 0;
            let trivial = vec![segment];
            if self.segments.is_empty() {
                return vec![trivial];
            } else {
                return vec![std::mem::take(&mut self.segments), trivial];
            }
        }

        self.total_block_count = 0;
        self.segments.push(segment);
        vec![std::mem::take(&mut self.segments)]
    }
}

struct CompactTaskBuilder {
    column_ids: HashSet<ColumnId>,
    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
}

impl CompactTaskBuilder {
    fn new(column_ids: HashSet<ColumnId>) -> Self {
        Self {
            column_ids,
            blocks: vec![],
            total_rows: 0,
            total_size: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn take_blocks(&mut self) -> Vec<Arc<BlockMeta>> {
        self.total_rows = 0;
        self.total_size = 0;
        std::mem::take(&mut self.blocks)
    }

    fn check_column_ids(&self, block: &Arc<BlockMeta>) -> bool {
        let column_ids: HashSet<ColumnId> = block.col_metas.keys().cloned().collect();
        self.column_ids == column_ids
    }

    fn add(&mut self, block: &Arc<BlockMeta>, thresholds: BlockThresholds) -> (bool, bool) {
        self.total_rows += block.row_count as usize;
        self.total_size += block.block_size as usize;

        if !thresholds.check_large_enough(self.total_rows, self.total_size) {
            // blocks < N
            self.blocks.push(block.clone());
            return (false, false);
        }

        if self.blocks.is_empty() {
            if self.check_column_ids(block) {
                self.total_rows = 0;
                self.total_size = 0;
                return (true, false);
            } else {
                self.blocks.push(block.clone());
                return (false, true);
            }
        }

        if thresholds.check_for_compact(self.total_rows, self.total_size) {
            // N <= blocks < 2N
            self.blocks.push(block.clone());
            (false, true)
        } else {
            // blocks > 2N
            (true, true)
        }
    }
}
