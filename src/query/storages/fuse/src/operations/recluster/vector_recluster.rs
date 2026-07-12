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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use indexmap::IndexSet;

use super::overlap_selection::OverlapSegmentItem;
use super::overlap_selection::OverlapSelector;
use super::overlap_selection::collect_overlap_window_segments;
use super::overlap_selection::identity_overlaps;
use super::overlap_selection::overlap_depth_order;
use super::overlap_selection::refine_scalar_windows_by_overlap;
use super::overlap_selection::select_overlap_only_segments;
use super::recluster_mutator::ReclusterBlock;
use super::recluster_mutator::ReclusterGroup;
use super::recluster_mutator::ReclusterMutator;
use super::recluster_mutator::ReclusterTaskCandidate;
use super::recluster_mutator::SelectedReclusterSegment;
use crate::SegmentLocation;
use crate::statistics::VectorClusterInfo;
use crate::statistics::cluster_stats_scalar_overlap;

struct VectorReclusterSegment {
    segment: SelectedReclusterSegment,
    block_metas: Vec<Arc<BlockMeta>>,
    stats: ClusterStatistics,
}

impl OverlapSegmentItem for VectorReclusterSegment {
    fn from_selected_segment(
        segment: SelectedReclusterSegment,
        stats: ClusterStatistics,
    ) -> Result<Self> {
        let block_metas = segment.info.block_metas()?;
        Ok(Self {
            segment,
            block_metas,
            stats,
        })
    }

    fn selected_segment(&self) -> &SelectedReclusterSegment {
        &self.segment
    }
}

struct VectorOverlapSelector {
    overlaps: Vec<IndexSet<usize>>,
    depth_order: Vec<(usize, usize)>,
    cursor: usize,
}

impl VectorOverlapSelector {
    fn new(overlaps: Vec<IndexSet<usize>>) -> Self {
        let depth_order = overlap_depth_order(&overlaps);
        Self {
            overlaps,
            depth_order,
            cursor: 0,
        }
    }
}

impl OverlapSelector for VectorOverlapSelector {
    const LABEL: &'static str = "vector";

    fn depth_threshold(&self, default_threshold: f64) -> f64 {
        default_threshold.min(1.0)
    }

    fn depth_summary(&self) -> (usize, usize) {
        let mut max_depth = 0usize;
        let mut sum_depth = 0usize;
        for overlap in &self.overlaps {
            let depth = overlap.len();
            max_depth = max_depth.max(depth);
            sum_depth += depth;
        }
        (max_depth, sum_depth)
    }

    fn next_seed(&mut self, used: &[bool]) -> Option<(usize, usize)> {
        while let Some((seed, _)) = self.depth_order.get(self.cursor).copied() {
            self.cursor += 1;
            let depth = self.overlaps[seed]
                .iter()
                .copied()
                .filter(|idx| !used[*idx])
                .count();
            if depth > 1 {
                return Some((seed, depth));
            }
        }
        None
    }

    fn members_by_priority(&self, seed: usize, used: &[bool]) -> Vec<usize> {
        let mut local_indices = self.overlaps[seed]
            .iter()
            .copied()
            .filter(|local_idx| !used[*local_idx])
            .collect::<Vec<_>>();
        local_indices.sort_by(|left, right| {
            self.overlaps[*right]
                .len()
                .cmp(&self.overlaps[*left].len())
                .then_with(|| left.cmp(right))
        });
        local_indices
    }
}

pub(super) fn fetch_vector_task_candidates(
    mutator: &ReclusterMutator,
    vector_cluster_info: &VectorClusterInfo,
    group: ReclusterGroup,
    indices: &[usize],
    blocks: &[&ReclusterBlock],
    task_budget: usize,
) -> Result<Vec<ReclusterTaskCandidate>> {
    let block_count = indices.len();
    let mut overlaps = identity_overlaps(block_count);
    let vector_stats = indices
        .iter()
        .map(|idx| block_meta_vector_stats(blocks[*idx].meta.as_ref(), vector_cluster_info))
        .collect::<Vec<_>>();
    let require_scalar_overlap = !mutator.cluster_key_types.is_empty();

    for left in 0..block_count {
        for right in left + 1..block_count {
            if require_scalar_overlap {
                let left_block = blocks[indices[left]];
                let right_block = blocks[indices[right]];
                if !cluster_stats_scalar_overlap(left_block.stats(), right_block.stats()) {
                    continue;
                }
            }

            let vector_overlap = match (vector_stats[left], vector_stats[right]) {
                (Some(left_stat), Some(right_stat)) => {
                    left_stat.spheres_overlap(right_stat, vector_cluster_info.distance_type)?
                }
                // Missing vector stats must stay conservative.
                _ => true,
            };
            if vector_overlap {
                overlaps[left].insert(right);
                overlaps[right].insert(left);
            }
        }
    }

    let selector = VectorOverlapSelector::new(overlaps);
    mutator.fetch_overlap_task_candidates(group, indices, blocks, selector, task_budget)
}

pub(super) fn select_vector_segments(
    mutator: &ReclusterMutator,
    vector_cluster_info: &VectorClusterInfo,
    compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
    window_len: usize,
) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
    if mutator.cluster_key_types.is_empty() {
        select_overlap_only_segments::<VectorReclusterSegment, VectorOverlapSelector, _>(
            mutator,
            compact_segments,
            window_len,
            |vector_segments| {
                let overlaps =
                    build_vector_segment_overlaps(vector_segments, vector_cluster_info, false)?;
                Ok(VectorOverlapSelector::new(overlaps))
            },
        )
    } else {
        let scalar_windows = mutator.select_scalar_segments(compact_segments, window_len)?;
        refine_scalar_windows_by_overlap(scalar_windows, window_len, |scalar_window| {
            let vector_segments =
                collect_overlap_window_segments::<VectorReclusterSegment>(mutator, scalar_window)?;

            let overlaps =
                build_vector_segment_overlaps(&vector_segments, vector_cluster_info, true)?;
            Ok((vector_segments, overlaps))
        })
    }
}

fn build_vector_segment_overlaps(
    vector_segments: &[VectorReclusterSegment],
    vector_cluster_info: &VectorClusterInfo,
    require_scalar_overlap: bool,
) -> Result<Vec<IndexSet<usize>>> {
    let mut overlaps = identity_overlaps(vector_segments.len());
    let segment_vector_stats = vector_segments
        .iter()
        .map(|segment| {
            let mut stats = Vec::with_capacity(segment.block_metas.len());
            let mut missing_stats = false;
            for block_meta in &segment.block_metas {
                if let Some(vector_stats) =
                    block_meta_vector_stats(block_meta.as_ref(), vector_cluster_info)
                {
                    stats.push(vector_stats);
                } else {
                    missing_stats = true;
                }
            }
            (stats, missing_stats)
        })
        .collect::<Vec<_>>();

    for left in 0..vector_segments.len() {
        for right in left + 1..vector_segments.len() {
            if require_scalar_overlap
                && !cluster_stats_scalar_overlap(
                    &vector_segments[left].stats,
                    &vector_segments[right].stats,
                )
            {
                continue;
            }

            let (left_stats, left_missing_stats) = &segment_vector_stats[left];
            let (right_stats, right_missing_stats) = &segment_vector_stats[right];
            let vector_overlap = if *left_missing_stats
                || *right_missing_stats
                || left_stats.is_empty()
                || right_stats.is_empty()
            {
                true
            } else {
                let mut overlap = false;
                'left_stats: for left_stat in left_stats {
                    for right_stat in right_stats {
                        if left_stat
                            .spheres_overlap(right_stat, vector_cluster_info.distance_type)?
                        {
                            overlap = true;
                            break 'left_stats;
                        }
                    }
                }
                overlap
            };

            if vector_overlap {
                overlaps[left].insert(right);
                overlaps[right].insert(left);
            }
        }
    }

    Ok(overlaps)
}

fn block_meta_vector_stats<'a>(
    block_meta: &'a BlockMeta,
    vector_cluster_info: &VectorClusterInfo,
) -> Option<&'a VectorColumnStatistics> {
    block_meta.vector_stats.as_ref()?.get(&(
        vector_cluster_info.column_id,
        vector_cluster_info.distance_type,
    ))
}
