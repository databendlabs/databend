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
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::Scalar;
use storages_common_table_meta::meta::BlockMeta;
use tracing::info;

use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::Replacement;
use crate::operations::common::ReplacementLogEntry;

#[derive(Clone)]
pub struct ReclusterMutator {
    ctx: Arc<dyn TableContext>,
    selected_blocks: Vec<Arc<BlockMeta>>,
    level: i32,
    threshold: f64,
    mutation_logs: MutationLogs,
    block_thresholds: BlockThresholds,
}

impl ReclusterMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        threshold: f64,
        block_thresholds: BlockThresholds,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
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
        let max_memory_usage =
            (self.ctx.get_settings().get_max_memory_usage()? as f64 * 0.5) as u64;
        for (level, block_metas) in blocks_map.into_iter() {
            if block_metas.len() <= 1 {
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: BTreeMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
            for (i, (_, meta)) in block_metas.iter().enumerate() {
                if let Some(stats) = &meta.cluster_stats {
                    points_map
                        .entry(stats.min.clone())
                        .and_modify(|v| v.0.push(i))
                        .or_insert((vec![i], vec![]));
                    points_map
                        .entry(stats.max.clone())
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
                        let entry = ReplacementLogEntry {
                            index: block_idx,
                            op: Replacement::Deleted,
                        };
                        self.mutation_logs
                            .entries
                            .push(MutationLogEntry::Replacement(entry));
                        block_meta
                    })
                    .collect();

                // Status.
                {
                    let status = format!(
                        "recluster: select block files: {}, total bytes: {}",
                        self.selected_blocks.len(),
                        total_bytes
                    );
                    self.ctx.set_status_info(&status);
                    info!(status);
                }
                self.level = level;

                return Ok(true);
            }

            let mut max_depth = 0;
            let mut max_points = Vec::new();
            let mut block_depths = Vec::new();
            let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
            let mut unfinished_parts: HashMap<usize, usize> = HashMap::new();
            for (i, (_, (start, end))) in points_map.into_iter().enumerate() {
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

                for (_, val) in unfinished_parts.iter_mut() {
                    *val = cmp::max(*val, point_depth);
                }

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
            tracing::debug!(
                "recluster: average_depth: {} in level {}",
                average_depth,
                level
            );
            if average_depth <= self.threshold {
                continue;
            }

            // find the max point, gather the blocks.
            let mut selected_idx = HashSet::new();
            point_overlaps[max_points[0]].iter().for_each(|idx| {
                selected_idx.insert(*idx);
            });

            let mut memory_usage = 0;
            for idx in selected_idx {
                let (block_idx, block_meta) = block_metas[idx].clone();
                if memory_usage >= max_memory_usage {
                    break;
                }
                memory_usage += block_meta.block_size;
                let entry = ReplacementLogEntry {
                    index: block_idx,
                    op: Replacement::Deleted,
                };
                self.mutation_logs
                    .entries
                    .push(MutationLogEntry::Replacement(entry));
                self.selected_blocks.push(block_meta);
            }

            // Status.
            {
                let status = format!(
                    "recluster: select block files: {}, total bytes: {}",
                    self.selected_blocks.len(),
                    memory_usage
                );
                self.ctx.set_status_info(&status);
                info!(status);
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
