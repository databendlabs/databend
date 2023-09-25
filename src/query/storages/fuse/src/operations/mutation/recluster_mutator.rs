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

use common_base::runtime::execute_futures_in_parallel;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::Scalar;
use indexmap::IndexSet;
use itertools::Itertools;
use log::error;
use minitrace::future::FutureExt;
use minitrace::Span;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::CompactSegmentInfo;
use storages_common_table_meta::meta::Statistics;

use crate::statistics::reducers::merge_statistics_mut;
use crate::table_functions::cmp_with_null;
use crate::SegmentLocation;

#[derive(Clone)]
pub struct ReclusterMutator {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) depth_threshold: f64,
    pub(crate) block_thresholds: BlockThresholds,
    pub(crate) cluster_key_id: u32,

    pub(crate) total_rows: usize,
    pub(crate) total_bytes: usize,
    pub(crate) level: i32,

    pub selected_blocks: Vec<Arc<BlockMeta>>,
    pub remained_blocks: Vec<Arc<BlockMeta>>,
    pub removed_segment_indexes: Vec<usize>,
    pub removed_segment_summary: Statistics,
}

impl ReclusterMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_id: u32,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            selected_blocks: Vec::new(),
            remained_blocks: Vec::new(),
            removed_segment_indexes: Vec::new(),
            removed_segment_summary: Statistics::default(),
            total_rows: 0,
            total_bytes: 0,
            level: 0,
        })
    }

    pub fn take_blocks(&mut self) -> Vec<Arc<BlockMeta>> {
        std::mem::take(&mut self.selected_blocks)
    }

    #[async_backtrace::framed]
    pub async fn target_select(
        &mut self,
        compact_segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    ) -> Result<bool> {
        let mut selected_segments = Vec::with_capacity(compact_segments.len());
        let mut selected_indices = Vec::with_capacity(compact_segments.len());
        let mut selected_statistics = Vec::with_capacity(compact_segments.len());
        compact_segments.into_iter().for_each(|(loc, info)| {
            selected_statistics.push(info.summary.clone());
            selected_segments.push(info);
            selected_indices.push(loc.segment_idx);
        });

        let blocks_map = self.gather_block_map(selected_segments).await?;
        if blocks_map.is_empty() {
            return Ok(false);
        }

        let mem_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;
        let recluster_block_size = self.ctx.get_settings().get_recluster_block_size()? as usize;
        let memory_threshold = recluster_block_size.min(mem_info.avail as usize * 1024 * 40 / 100);

        let mut remained_blocks = Vec::new();
        let mut selected = false;
        for (level, block_metas) in blocks_map.into_iter() {
            let len = block_metas.len();
            if level == -1 || selected || len < 2 {
                remained_blocks.extend(block_metas.into_iter());
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
            for (i, meta) in block_metas.iter().enumerate() {
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
                self.selected_blocks = block_metas;
                self.total_rows = total_rows as usize;
                self.total_bytes = total_bytes as usize;
                self.level = level;
                selected = true;
                continue;
            }

            let selected_idx = Self::fetch_max_depth(points_map, self.depth_threshold)?;
            if selected_idx.is_empty() {
                remained_blocks.extend(block_metas.into_iter());
                continue;
            }

            let blocks_idx: IndexSet<usize> = IndexSet::from_iter(0..len);
            let diff = blocks_idx.difference(&selected_idx);
            diff.into_iter()
                .for_each(|v| remained_blocks.push(block_metas[*v].clone()));

            let mut over_memory = false;
            for idx in selected_idx {
                let block_meta = block_metas[idx].clone();
                if over_memory {
                    remained_blocks.push(block_meta);
                    continue;
                }

                let memory_usage = self.total_bytes + block_meta.block_size as usize;
                if memory_usage > memory_threshold {
                    remained_blocks.push(block_meta);
                    over_memory = true;
                    continue;
                }

                self.total_rows += block_meta.row_count as usize;
                self.total_bytes = memory_usage;
                self.selected_blocks.push(block_meta);
            }

            self.level = level;
            selected = true;
        }

        if selected {
            self.remained_blocks = remained_blocks;

            selected_indices.sort_by(|a, b| b.cmp(a));
            self.removed_segment_indexes = selected_indices;

            let default_cluster_key_id = Some(self.cluster_key_id);
            selected_statistics.iter().for_each(|v| {
                merge_statistics_mut(&mut self.removed_segment_summary, v, default_cluster_key_id)
            });
        }
        Ok(selected)
    }

    pub fn select_segments(
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        block_per_seg: usize,
        max_len: usize,
        cluster_key_id: u32,
    ) -> Result<IndexSet<usize>> {
        let mut blocks_num = 0;
        let mut indices = IndexSet::new();
        let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
        for (i, (_, compact_segment)) in compact_segments.iter().enumerate() {
            if !ReclusterMutator::segment_can_recluster(
                &compact_segment.summary,
                block_per_seg,
                cluster_key_id,
            ) {
                continue;
            }

            if let Some(stats) = &compact_segment.summary.cluster_stats {
                blocks_num += compact_segment.summary.block_count as usize;
                indices.insert(i);
                points_map
                    .entry(stats.min())
                    .and_modify(|v| v.0.push(i))
                    .or_insert((vec![i], vec![]));
                points_map
                    .entry(stats.max())
                    .and_modify(|v| v.1.push(i))
                    .or_insert((vec![], vec![i]));
            }
        }

        if indices.len() < 2 || blocks_num < block_per_seg {
            return Ok(indices);
        }

        let mut selected_segs = ReclusterMutator::fetch_max_depth(points_map, 1.0)?;
        selected_segs.truncate(max_len);
        Ok(selected_segs)
    }

    pub fn segment_can_recluster(
        summary: &Statistics,
        block_per_seg: usize,
        cluster_key_id: u32,
    ) -> bool {
        if let Some(stats) = &summary.cluster_stats {
            stats.cluster_key_id == cluster_key_id
                && (stats.level >= 0 || (summary.block_count as usize) >= block_per_seg)
        } else {
            false
        }
    }

    #[async_backtrace::framed]
    async fn gather_block_map(
        &self,
        compact_segments: Vec<Arc<CompactSegmentInfo>>,
    ) -> Result<BTreeMap<i32, Vec<Arc<BlockMeta>>>> {
        // combine all the tasks.
        let mut iter = compact_segments.into_iter();
        let tasks = std::iter::from_fn(|| {
            iter.next().map(|v| {
                async move {
                    v.block_metas()
                        .map_err(|_| ErrorCode::Internal("Failed to get block metas"))
                }
                .in_span(Span::enter_with_local_parent("try_from_segments"))
            })
        });

        let thread_nums = self.ctx.get_settings().get_max_threads()? as usize;

        let blocks = execute_futures_in_parallel(
            tasks,
            thread_nums,
            thread_nums,
            "convert-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let mut blocks_map: BTreeMap<i32, Vec<Arc<BlockMeta>>> = BTreeMap::new();
        for block in blocks.into_iter().flatten() {
            match &block.cluster_stats {
                Some(stats) if stats.cluster_key_id == self.cluster_key_id => {
                    blocks_map.entry(stats.level).or_default().push(block)
                }
                _ => {
                    return Ok(BTreeMap::new());
                }
            }
        }
        Ok(blocks_map)
    }

    fn fetch_max_depth(
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
    ) -> Result<IndexSet<usize>> {
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
            let point_depth = if unfinished_parts.len() == 1 && Self::check_point(&start, &end) {
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
        assert!(!max_points.is_empty());
        if !unfinished_parts.is_empty() {
            error!("Recluster: unfinished_parts is not empty after calculate the blocks overlaps");
            return Err(ErrorCode::Internal(
                "failed to select blocks for recluster, please check your data",
            ));
        }

        let sum_depth: usize = block_depths.iter().sum();
        // round the float to 4 decimal places.
        let average_depth =
            (10000.0 * sum_depth as f64 / block_depths.len() as f64).round() / 10000.0;

        // find the max point, gather the indices.
        let mut selected_idx = IndexSet::new();
        if average_depth > depth_threshold {
            let mut point = max_points[0];
            point_overlaps[point].iter().for_each(|idx| {
                selected_idx.insert(*idx);
            });
            for next in max_points.into_iter().skip(1) {
                point += 1;
                if next != point {
                    break;
                }

                point_overlaps[next].iter().for_each(|idx| {
                    selected_idx.insert(*idx);
                });
            }
        }
        Ok(selected_idx)
    }

    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    pub fn check_point(start: &[usize], end: &[usize]) -> bool {
        if start.len() + end.len() > 3 || start.is_empty() || end.is_empty() {
            return false;
        }

        let set: HashSet<usize> = HashSet::from_iter(start.iter().chain(end.iter()).cloned());
        set.len() == 2
    }
}
