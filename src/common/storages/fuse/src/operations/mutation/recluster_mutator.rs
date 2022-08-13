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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::TableSnapshot;

use crate::io::MetaReaders;
use crate::sessions::TableContext;

pub struct ReclusterMutator<'a> {
    ctx: &'a Arc<dyn TableContext>,
    base_snapshot: &'a TableSnapshot,
    threshold: f64,
    row_per_block: usize,
}

impl<'a> ReclusterMutator<'a> {
    async fn block_select_task(&mut self) -> Result<Vec<(usize, BlockMeta)>> {
        let snapshot = self.base_snapshot;

        let default_cluster_key_id = snapshot
            .cluster_key_meta
            .clone()
            .ok_or_else(|| {
                ErrorCode::InvalidClusterKeys("Invalid clustering keys or table is not clustered")
            })?
            .0;

        let mut blocks_map = BTreeMap::new();
        let reader = MetaReaders::segment_info_reader(self.ctx.as_ref());
        for (idx, segment_location) in snapshot.segments.iter().enumerate() {
            let (x, ver) = (segment_location.0.clone(), segment_location.1);
            let segment = reader.read(x, None, ver).await?;

            segment.blocks.iter().for_each(|b| {
                if let Some(stats) = &b.cluster_stats {
                    if stats.cluster_key_id == default_cluster_key_id && stats.level >= 0 {
                        blocks_map
                            .entry(stats.level)
                            .or_insert(Vec::new())
                            .push((idx, b.clone()));
                    }
                }
            });
        }

        for block_metas in blocks_map.values() {
            let mut total_row_count = 0;
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

                total_row_count += meta.row_count;
            }

            if total_row_count <= self.row_per_block as u64 {
                return Ok(block_metas.clone());
            }

            let mut max_depth = 0;
            let mut block_depths = Vec::new();
            let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
            let mut unfinished_parts: HashMap<usize, usize> = HashMap::new();
            for (start, end) in points_map.values() {
                let point_depth = unfinished_parts.len() + start.len();
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
            let mut selected_idx = Vec::new();
            let mut find = false;
            for overlap in point_overlaps {
                if overlap.len() == max_depth {
                    let mut blocks = overlap.clone();
                    selected_idx.append(&mut blocks);
                    find = true;
                } else if find {
                    break;
                }
            }
            selected_idx.dedup();

            let selected_blocks = selected_idx
                .iter()
                .map(|idx| block_metas[*idx].clone())
                .collect::<Vec<_>>();
            return Ok(selected_blocks);
        }

        Ok(vec![])
    }
}
