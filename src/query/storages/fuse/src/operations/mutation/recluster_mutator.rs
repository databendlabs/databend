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
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::Scalar;
use itertools::Itertools;
use log::debug;
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::table_functions::cmp_with_null;

#[derive(Clone)]
pub struct ReclusterMutator {
    memory_threshold: usize,
    depth_threshold: f64,
    block_thresholds: BlockThresholds,

    mutation_logs: MutationLogs,
    selected_blocks: Vec<Arc<BlockMeta>>,

    pub(crate) total_rows: usize,
    pub(crate) total_bytes: usize,
    pub(crate) level: i32,
}

impl ReclusterMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
    ) -> Result<Self> {
        let mem_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;
        let max_memory_usage = ctx.get_settings().get_max_memory_usage()? as usize;
        let memory_threshold =
            cmp::min(mem_info.avail as usize * 1024, max_memory_usage) * 50 / 100;
        Ok(Self {
            memory_threshold,
            depth_threshold,
            block_thresholds,
            mutation_logs: Default::default(),
            selected_blocks: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            level: 0,
        })
    }

    pub fn take_blocks(&mut self) -> Vec<Arc<BlockMeta>> {
        std::mem::take(&mut self.selected_blocks)
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
            if block_metas.len() < 2 {
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
            for (i, (_, meta)) in block_metas.iter().enumerate() {
                if let Some(stats) = &meta.cluster_stats {
                    points_map
                        .entry(stats.min())
                        .and_modify(|v| v.0.push(i))
                        .or_insert((vec![i], vec![]));
                    points_map
                        .entry(stats.max())
                        .and_modify(|v| v.1.push(i))
                        .or_insert((vec![], vec![i]));
                }
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
                        self.mutation_logs
                            .entries
                            .push(MutationLogEntry::DeletedBlock { index: block_idx });
                        block_meta
                    })
                    .collect();
                self.total_rows = total_rows as usize;
                self.total_bytes = total_bytes as usize;
                self.level = level;
                return Ok(true);
            }

            let mut max_depth = 0;
            let mut max_points = Vec::new();
            let mut block_depths = Vec::new();
            let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
            let mut unfinished_parts: HashMap<usize, usize> = HashMap::new();
            for (i, (_, (start, end))) in points_map
                .into_iter()
                .sorted_by(|(a, _), (b, _)| a.iter().cmp_by(b.iter(), cmp_with_null))
                .enumerate()
            {
                let point_depth = if unfinished_parts.len() == 1 && Self::check_point(&start, &end)
                {
                    1
                } else {
                    unfinished_parts.len() + start.len()
                };

                match point_depth.cmp(&max_depth) {
                    Ordering::Greater => {
                        max_depth = point_depth;
                        max_points = vec![i];
                    }
                    Ordering::Equal => max_points.push(i),
                    Ordering::Less => (),
                }

                unfinished_parts
                    .values_mut()
                    .for_each(|val| *val = cmp::max(*val, point_depth));

                start.iter().for_each(|&idx| {
                    unfinished_parts.insert(idx, point_depth);
                });

                point_overlaps.push(unfinished_parts.keys().cloned().collect());

                end.iter().for_each(|idx| {
                    if let Some(v) = unfinished_parts.remove(idx) {
                        block_depths.push(v);
                    }
                });
            }
            assert!(unfinished_parts.is_empty());
            assert!(!max_points.is_empty());

            let sum_depth: usize = block_depths.iter().sum();
            // round the float to 4 decimal places.
            let average_depth =
                (10000.0 * sum_depth as f64 / block_depths.len() as f64).round() / 10000.0;
            debug!(
                "recluster: average_depth: {} in level {}",
                average_depth, level
            );
            if average_depth <= self.depth_threshold {
                continue;
            }

            // find the max point, gather the blocks.
            let mut selected_idx = HashSet::new();
            point_overlaps[max_points[0]].iter().for_each(|idx| {
                selected_idx.insert(*idx);
            });

            for idx in selected_idx {
                let (block_idx, block_meta) = block_metas[idx].clone();
                let memory_usage = self.total_bytes + block_meta.block_size as usize;
                if memory_usage > self.memory_threshold {
                    break;
                }

                self.mutation_logs
                    .entries
                    .push(MutationLogEntry::DeletedBlock { index: block_idx });
                self.total_rows += block_meta.row_count as usize;
                self.total_bytes = memory_usage;
                self.selected_blocks.push(block_meta);
            }

            self.level = level;
            return Ok(true);
        }

        Ok(false)
    }

    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    fn check_point(start: &[usize], end: &[usize]) -> bool {
        if start.len() + end.len() > 3 || start.is_empty() || end.is_empty() {
            return false;
        }

        let set: HashSet<usize> = HashSet::from_iter(start.iter().chain(end.iter()).cloned());
        set.len() == 2
    }
}
