// Copyright 2021 Datafuse Labs
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

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::Scalar;
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::Replacement;
use crate::operations::common::ReplacementLogEntry;

static MAX_BLOCK_COUNT: usize = 50;

#[derive(Clone)]
pub struct ReclusterMutator {
    selected_blocks: Vec<Arc<BlockMeta>>,
    level: i32,
    threshold: f64,
    mutation_logs: MutationLogs,
    block_thresholds: BlockThresholds,
}

impl ReclusterMutator {
    pub fn try_create(threshold: f64, block_thresholds: BlockThresholds) -> Result<Self> {
        Ok(Self {
            selected_blocks: Vec::new(),
            level: 0,
            threshold,
            block_thresholds,
            mutation_logs: Default::default(),
        })
    }

    pub fn selected_blocks(&self) -> Vec<Arc<BlockMeta>> {
        self.selected_blocks.clone()
    }

    pub fn level(&self) -> i32 {
        self.level
    }

    pub fn mutation_logs(&self) -> MutationLogs {
        self.mutation_logs.clone()
    }

    #[async_backtrace::framed]
    pub async fn target_select(
        &mut self,
        blocks_map: BTreeMap<i32, Vec<(BlockMetaIndex, Arc<BlockMeta>)>>,
    ) -> Result<bool> {
        for (level, block_metas) in blocks_map.into_iter() {
            if block_metas.len() <= 1 {
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: BTreeMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
            for (i, (_, meta)) in block_metas.iter().enumerate() {
                let stats = meta.cluster_stats.clone().unwrap();
                points_map
                    .entry(stats.min.clone())
                    .and_modify(|v| v.0.push(i))
                    .or_insert((vec![i], vec![]));
                points_map
                    .entry(stats.max.clone())
                    .and_modify(|v| v.1.push(i))
                    .or_insert((vec![], vec![i]));

                total_rows += meta.row_count;
                total_bytes += meta.block_size;
            }

            // If the statistics of blocks are too small, just merge them into one block.
            if self
                .block_thresholds
                .check_for_recluster(total_rows as usize, total_bytes as usize)
            {
                self.selected_blocks = block_metas
                    .into_iter()
                    .map(|(block_idx, block_meta)| {
                        let entry = ReplacementLogEntry {
                            index: block_idx,
                            op: Replacement::Deleted,
                        };
                        self.mutation_logs
                            .entries
                            .push(MutationLogEntry::Replacement(entry));
                        block_meta
                    })
                    .collect::<Vec<_>>();
                self.level = level;

                return Ok(true);
            }

            let mut max_depth = 0;
            let mut block_depths = Vec::new();
            let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
            let mut unfinished_parts: HashMap<usize, usize> = HashMap::new();
            for (start, end) in points_map.values() {
                // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
                let point_depth =
                    if unfinished_parts.len() == 1 && start.len() == 1 && end.len() == 1 {
                        1
                    } else {
                        unfinished_parts.len() + start.len()
                    };

                if point_depth > max_depth {
                    max_depth = point_depth;
                }

                for (_, val) in unfinished_parts.iter_mut() {
                    *val = cmp::max(*val, point_depth);
                }

                start.iter().for_each(|&idx| {
                    unfinished_parts.insert(idx, point_depth);
                });

                point_overlaps.push(unfinished_parts.keys().cloned().collect());

                end.iter().for_each(|&idx| {
                    let stat = unfinished_parts.remove(&idx).unwrap();
                    block_depths.push(stat);
                });
            }
            assert_eq!(unfinished_parts.len(), 0);

            let sum_depth: usize = block_depths.iter().sum();
            // round the float to 4 decimal places.
            let average_depth =
                (10000.0 * sum_depth as f64 / block_depths.len() as f64).round() / 10000.0;
            if average_depth <= self.threshold {
                continue;
            }

            // find the max point, gather the blocks.
            let mut selected_idx = HashSet::new();
            let mut find = false;
            for overlap in point_overlaps {
                if overlap.len() == max_depth {
                    overlap.iter().for_each(|&idx| {
                        selected_idx.insert(idx);
                    });
                    find = true;
                } else if find {
                    break;
                }
            }

            self.selected_blocks = selected_idx
                .iter()
                .take(MAX_BLOCK_COUNT)
                .map(|idx| {
                    let (block_idx, block_meta) = block_metas[*idx].clone();
                    let entry = ReplacementLogEntry {
                        index: block_idx,
                        op: Replacement::Deleted,
                    };
                    self.mutation_logs
                        .entries
                        .push(MutationLogEntry::Replacement(entry));
                    block_meta
                })
                .collect::<Vec<_>>();
            self.level = level;
            return Ok(true);
        }

        Ok(false)
    }
}
