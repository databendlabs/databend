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

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use common_meta_app::schema::TableInfo;

use crate::io::BlockCompactor;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::BaseMutator;
use crate::operations::AppendOperationLogEntry;
use crate::sessions::TableContext;
use crate::statistics::merge_statistics;
use crate::FuseTable;
use crate::TableMutator;

static MAX_BLOCK_COUNT: usize = 50;

#[derive(Clone)]
pub struct ReclusterMutator {
    base_mutator: BaseMutator,
    blocks_map: BTreeMap<i32, Vec<(usize, BlockMeta)>>,
    selected_blocks: Vec<BlockMeta>,
    level: i32,
    block_compactor: BlockCompactor,
    threshold: f64,
}

impl ReclusterMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        location_generator: TableMetaLocationGenerator,
        base_snapshot: Arc<TableSnapshot>,
        threshold: f64,
        block_compactor: BlockCompactor,
        blocks_map: BTreeMap<i32, Vec<(usize, BlockMeta)>>,
    ) -> Result<Self> {
        let base_mutator = BaseMutator::try_create(ctx, location_generator, base_snapshot)?;
        Ok(Self {
            base_mutator,
            blocks_map,
            selected_blocks: Vec::new(),
            level: 0,
            block_compactor,
            threshold,
        })
    }

    pub fn partitions_total(&self) -> usize {
        self.base_mutator.base_snapshot.summary.block_count as usize
    }

    pub fn selected_blocks(&self) -> Vec<BlockMeta> {
        self.selected_blocks.clone()
    }

    pub fn level(&self) -> i32 {
        self.level
    }
}

#[async_trait::async_trait]
impl TableMutator for ReclusterMutator {
    async fn blocks_select(&mut self) -> Result<bool> {
        let blocks_map = self.blocks_map.clone();
        for (level, block_metas) in blocks_map.into_iter() {
            if block_metas.len() <= 1 {
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: BTreeMap<Vec<DataValue>, (Vec<usize>, Vec<usize>)> =
                BTreeMap::new();
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
                .block_compactor
                .check_for_recluster(total_rows as usize, total_bytes as usize)
            {
                self.selected_blocks = block_metas
                    .into_iter()
                    .map(|(seg_idx, block_meta)| {
                        self.base_mutator
                            .add_mutation(seg_idx, block_meta.location.clone(), None);
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
                    let (seg_idx, block_meta) = block_metas[*idx].clone();
                    self.base_mutator
                        .add_mutation(seg_idx, block_meta.location.clone(), None);
                    block_meta
                })
                .collect::<Vec<_>>();
            self.level = level;
            return Ok(true);
        }

        Ok(false)
    }

    async fn try_commit(&self, table_info: &TableInfo) -> Result<()> {
        let base_mutator = self.base_mutator.clone();
        let ctx = base_mutator.ctx.clone();
        let (mut segments, mut summary) = self.base_mutator.generate_segments().await?;

        let append_entries = ctx.consume_precommit_blocks();
        let append_log_entries = append_entries
            .iter()
            .map(AppendOperationLogEntry::try_from)
            .collect::<Result<Vec<AppendOperationLogEntry>>>()?;

        let (merged_segments, merged_summary) =
            FuseTable::merge_append_operations(&append_log_entries)?;

        let mut merged_segments = merged_segments
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();

        segments.append(&mut merged_segments);
        summary = merge_statistics(&summary, &merged_summary)?;

        let new_snapshot = base_mutator.into_new_snapshot(segments, summary).await?;

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            table_info,
            &self.base_mutator.location_generator,
            new_snapshot,
        )
        .await?;
        Ok(())
    }
}
