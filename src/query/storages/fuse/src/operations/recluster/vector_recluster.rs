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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnRef;
use databend_common_expression::Expr;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use indexmap::IndexSet;
use log::debug;

use crate::FuseTable;
use crate::SegmentLocation;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::ReclusterTaskKind;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::passes_depth_gate;
use crate::operations::recluster::select_scalar_segments;
use crate::operations::recluster::task_candidate;
use crate::statistics::VectorClusterInfo;
use crate::statistics::vector_cluster_info_from_column;

// Vector component discovery is still seeded by overlap depth, but FINAL uses
// the density score plus tail eligibility to decide which task is worth one
// task slot. Probe a bounded component pool before enforcing `task_budget`, so a
// high-depth but inefficient component cannot hide a lower-depth eligible one.
const MIN_VECTOR_COMPONENT_PROBE_COUNT: usize = 32;
const VECTOR_COMPONENT_PROBE_FACTOR: usize = 4;

/// Vector cluster-key recluster behavior.
pub(crate) struct VectorReclusterStrategy {
    info: VectorClusterInfo,
}

impl VectorReclusterStrategy {
    pub(crate) fn try_create(
        table: &FuseTable,
        cluster_key_exprs: &mut Vec<Expr<usize>>,
        vector_index: usize,
    ) -> Result<Self> {
        let expr = &cluster_key_exprs[vector_index];
        let DataType::Vector(vector_ty) = expr.data_type().remove_nullable() else {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key must be vector type",
            ));
        };
        let Expr::ColumnRef(ColumnRef { id, .. }) = expr else {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key only supports direct column reference",
            ));
        };
        let table_schema = table.schema();
        let field = table_schema.field(*id);
        let dimension: usize = vector_ty.dimension().try_into().map_err(|_| {
            ErrorCode::InvalidClusterKeys("Vector cluster key dimension is too large for kmeans")
        })?;
        if dimension == 0 {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key dimension must be greater than zero",
            ));
        }
        let info = vector_cluster_info_from_column(
            &table.table_info.meta.indexes,
            vector_index,
            field.column_id(),
            field.name(),
            dimension,
        )?;
        cluster_key_exprs.remove(vector_index);
        Ok(Self { info })
    }

    pub(crate) fn new(info: VectorClusterInfo) -> Self {
        Self { info }
    }

    fn collect_segments(
        &self,
        properties: &ReclusterProperties,
        segments: Vec<SelectedReclusterSegment>,
    ) -> Result<Vec<VectorReclusterSegment>> {
        segments
            .into_iter()
            .map(|segment| {
                let stats = self.build_cluster_stats(
                    properties,
                    segment.info.summary.cluster_stats.as_ref(),
                    &segment.info.summary.col_stats,
                );
                VectorReclusterSegment::new(segment, stats)
            })
            .collect()
    }

    fn select_overlap_only_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        let window_len = window_len.max(1);
        let block_per_segment = properties.block_thresholds.block_per_segment;
        let mut total_blocks = 0usize;
        let mut segments = Vec::with_capacity(compact_segments.len());

        for (loc, info) in compact_segments {
            let block_count = info.summary.block_count as usize;
            let stats = self.build_cluster_stats(
                properties,
                info.summary.cluster_stats.as_ref(),
                &info.summary.col_stats,
            );
            if stats.level < 0 && block_count >= block_per_segment {
                continue;
            }
            total_blocks += block_count;
            segments.push(VectorReclusterSegment::new(
                SelectedReclusterSegment {
                    loc: loc.clone(),
                    info: info.clone(),
                },
                stats,
            )?);
        }

        let mut windows = Vec::new();
        let mut seen_windows = HashSet::new();
        let mut covered = vec![false; segments.len()];
        if segments.len() > 1 && total_blocks > block_per_segment {
            let overlaps = build_vector_segment_overlaps(&segments, &self.info, false)?;
            let mut selector = VectorOverlapSelector::new(overlaps);
            while let Some(indices) = selector.next_window(window_len, &mut seen_windows, &covered)
            {
                for &idx in &indices {
                    covered[idx] = true;
                }
                windows.push(indices);
            }
            debug!(
                "recluster: vector segment selection overlap windows segments={} blocks={} window_count={} covered_segments={}",
                segments.len(),
                total_blocks,
                windows.len(),
                covered.iter().filter(|covered| **covered).count(),
            );
        }

        let mut fallback = Vec::with_capacity(window_len);
        for (idx, is_covered) in covered.iter().enumerate() {
            if *is_covered {
                continue;
            }
            fallback.push(idx);
            if fallback.len() == window_len {
                if seen_windows.insert(fallback.clone()) {
                    windows.push(std::mem::replace(
                        &mut fallback,
                        Vec::with_capacity(window_len),
                    ));
                } else {
                    fallback.clear();
                }
            }
        }
        if !fallback.is_empty() && seen_windows.insert(fallback.clone()) {
            windows.push(fallback);
        }

        Ok(windows
            .into_iter()
            .filter(|window| !window.is_empty())
            .map(|window| {
                window
                    .into_iter()
                    .map(|idx| segments[idx].segment.clone())
                    .collect()
            })
            .collect())
    }

    fn refine_scalar_windows(
        &self,
        properties: &ReclusterProperties,
        scalar_windows: Vec<Vec<SelectedReclusterSegment>>,
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        let window_len = window_len.max(1);
        let mut windows = Vec::with_capacity(scalar_windows.len());
        let mut seen_windows = HashSet::new();

        for scalar_window in scalar_windows {
            if scalar_window.len() < 2 {
                let key = scalar_window
                    .iter()
                    .map(|segment| segment.loc.segment_idx)
                    .collect::<Vec<_>>();
                if seen_windows.insert(key) {
                    windows.push(scalar_window);
                }
                continue;
            }

            let segments = self.collect_segments(properties, scalar_window)?;
            let overlaps = build_vector_segment_overlaps(&segments, &self.info, true)?;
            let mut visited = vec![false; segments.len()];
            for start in 0..segments.len() {
                if visited[start] {
                    continue;
                }
                let mut stack = vec![start];
                let mut component = Vec::new();
                visited[start] = true;
                while let Some(idx) = stack.pop() {
                    component.push(idx);
                    for next in overlaps[idx].iter().copied() {
                        if !visited[next] {
                            visited[next] = true;
                            stack.push(next);
                        }
                    }
                }

                component.sort_unstable_by_key(|idx| segments[*idx].segment.loc.segment_idx);
                for chunk in component.chunks(window_len) {
                    let key = chunk
                        .iter()
                        .map(|idx| segments[*idx].segment.loc.segment_idx)
                        .collect::<Vec<_>>();
                    if seen_windows.insert(key) {
                        windows.push(
                            chunk
                                .iter()
                                .map(|idx| segments[*idx].segment.clone())
                                .collect(),
                        );
                    }
                }
            }
        }
        Ok(windows)
    }
}

impl ReclusterStrategy for VectorReclusterStrategy {
    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        if properties.scalar_cluster_key_types.is_empty() {
            self.select_overlap_only_segments(properties, compact_segments, window_len)
        } else {
            let scalar_windows =
                select_scalar_segments(self, properties, compact_segments, window_len)?;
            self.refine_scalar_windows(properties, scalar_windows, window_len)
        }
    }

    fn fetch_task_candidates(
        &self,
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        let block_count = indices.len();
        if block_count < 2 || task_budget == 0 {
            return Ok(Vec::new());
        }

        let mut overlaps = identity_overlaps(block_count);
        let vector_stats = indices
            .iter()
            .map(|idx| block_meta_vector_stats(blocks[*idx].meta.as_ref(), &self.info))
            .collect::<Vec<_>>();
        let require_scalar_overlap = !properties.scalar_cluster_key_types.is_empty();

        for left in 0..block_count {
            for right in left + 1..block_count {
                if require_scalar_overlap
                    && !scalar_cluster_stats_overlap(
                        blocks[indices[left]].stats(),
                        blocks[indices[right]].stats(),
                    )
                {
                    continue;
                }
                let vector_overlap = match (vector_stats[left], vector_stats[right]) {
                    (Some(left_stat), Some(right_stat)) => {
                        left_stat.spheres_overlap(right_stat, self.info.distance_type)?
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

        let (max_depth, sum_depth) = overlaps
            .iter()
            .map(IndexSet::len)
            .fold((0, 0), |(max_depth, sum_depth), depth| {
                (max_depth.max(depth), sum_depth + depth)
            });
        let average_depth = (10000.0 * sum_depth as f64 / block_count as f64).round() / 10000.0;
        // Vector overlap uses pairwise spheres, so any overlapping pair qualifies.
        let depth_threshold = properties.depth_threshold.min(1.0);
        if !passes_depth_gate(depth_threshold, average_depth, max_depth) {
            debug!(
                "recluster: vector candidate selection group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=below_vector_depth_gate",
                group, block_count, average_depth, max_depth,
            );
            return Ok(Vec::new());
        }

        let mut selector = VectorOverlapSelector::new(overlaps);
        let mut candidates = Vec::new();
        let mut used = vec![false; block_count];
        let candidate_probe_count = if properties.mode == ReclusterMode::Aggressive {
            task_budget
                .saturating_mul(VECTOR_COMPONENT_PROBE_FACTOR)
                .max(MIN_VECTOR_COMPONENT_PROBE_COUNT)
        } else {
            task_budget
        };
        while candidates.len() < candidate_probe_count {
            let Some((seed, depth)) = selector.next_seed(&used) else {
                break;
            };
            let mut task_bytes = 0usize;
            let mut selected = Vec::new();
            for local_idx in selector.members(seed, &used) {
                let block_size = blocks[indices[local_idx]].meta.block_size as usize;
                if !selected.is_empty()
                    && task_bytes.saturating_add(block_size) > properties.memory_threshold
                {
                    break;
                }
                task_bytes = task_bytes.saturating_add(block_size);
                selected.push(local_idx);
            }
            if selected.len() < 2 {
                continue;
            }
            let estimated_depth_gain = selector.estimate_depth_gain(&selected);
            for &local_idx in &selected {
                used[local_idx] = true;
            }
            let task_depth = depth.min(selected.len());
            let task_indices = selected
                .into_iter()
                .map(|local_idx| indices[local_idx])
                .collect::<Vec<_>>();
            candidates.push(task_candidate(
                group,
                CandidateScore {
                    selected_total_bytes: task_bytes,
                    selected_block_count: task_indices.len(),
                    max_depth: task_depth,
                    average_depth,
                    estimated_depth_gain,
                },
                &task_indices,
                blocks,
                ReclusterTaskKind::Recluster,
            ));
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp_desc(&left.score)
                .then_with(|| left.output_level.cmp(&right.output_level))
        });
        if properties.mode == ReclusterMode::Aggressive {
            candidates.retain(|candidate| candidate.passes_aggressive_tail_filter());
        }
        candidates.truncate(task_budget);

        debug!(
            "recluster: vector candidate selection group={} block_count={} avg_depth={} depth_threshold={} max_depth={} candidate_probe_count={} task_count={}",
            group,
            block_count,
            average_depth,
            depth_threshold,
            max_depth,
            candidate_probe_count,
            candidates.len(),
        );
        Ok(candidates)
    }
}

struct VectorReclusterSegment {
    segment: SelectedReclusterSegment,
    block_metas: Vec<Arc<BlockMeta>>,
    stats: ClusterStatistics,
}

impl VectorReclusterSegment {
    fn new(segment: SelectedReclusterSegment, stats: ClusterStatistics) -> Result<Self> {
        let block_metas = segment.info.block_metas()?;
        Ok(Self {
            segment,
            block_metas,
            stats,
        })
    }
}

struct VectorOverlapSelector {
    overlaps: Vec<IndexSet<usize>>,
    depth_order: Vec<(usize, usize)>,
    cursor: usize,
}

impl VectorOverlapSelector {
    fn new(overlaps: Vec<IndexSet<usize>>) -> Self {
        let mut depth_order = overlaps
            .iter()
            .enumerate()
            .map(|(idx, overlap)| (idx, overlap.len()))
            .filter(|(_, depth)| *depth > 1)
            .collect::<Vec<_>>();
        depth_order.sort_by(|(left_idx, left_depth), (right_idx, right_depth)| {
            right_depth
                .cmp(left_depth)
                .then_with(|| left_idx.cmp(right_idx))
        });
        Self {
            overlaps,
            depth_order,
            cursor: 0,
        }
    }

    fn next_seed(&mut self, used: &[bool]) -> Option<(usize, usize)> {
        while let Some((seed, _)) = self.depth_order.get(self.cursor).copied() {
            self.cursor += 1;
            let depth = self.overlaps[seed]
                .iter()
                .filter(|idx| !used[**idx])
                .count();
            if depth > 1 {
                return Some((seed, depth));
            }
        }
        None
    }

    fn members(&self, seed: usize, used: &[bool]) -> Vec<usize> {
        let mut members = self.overlaps[seed]
            .iter()
            .copied()
            .filter(|idx| !used[*idx])
            .collect::<Vec<_>>();
        members.sort_by(|left, right| {
            self.overlaps[*right]
                .len()
                .cmp(&self.overlaps[*left].len())
                .then_with(|| left.cmp(right))
        });
        members
    }

    fn estimate_depth_gain(&self, selected: &[usize]) -> u64 {
        let mut gain = 0u64;
        for (pos, &left) in selected.iter().enumerate() {
            for &right in &selected[pos + 1..] {
                if self.overlaps[left].contains(&right) {
                    gain += 1;
                }
            }
        }
        gain
    }

    fn next_window(
        &mut self,
        window_len: usize,
        seen_windows: &mut HashSet<Vec<usize>>,
        covered: &[bool],
    ) -> Option<Vec<usize>> {
        while let Some((seed, _)) = self.next_seed(covered) {
            let mut selected = self.members(seed, covered);
            selected.truncate(window_len);
            if selected.len() < 2 {
                continue;
            }
            let mut key = selected.clone();
            key.sort_unstable();
            if seen_windows.insert(key) {
                return Some(selected);
            }
        }
        None
    }
}

/// Seed an overlap graph where every node starts overlapping only itself.
fn identity_overlaps(len: usize) -> Vec<IndexSet<usize>> {
    (0..len).map(|idx| IndexSet::from_iter([idx])).collect()
}

fn build_vector_segment_overlaps(
    segments: &[VectorReclusterSegment],
    info: &VectorClusterInfo,
    require_scalar_overlap: bool,
) -> Result<Vec<IndexSet<usize>>> {
    let mut overlaps = identity_overlaps(segments.len());
    let segment_vector_stats = segments
        .iter()
        .map(|segment| {
            let mut stats = Vec::with_capacity(segment.block_metas.len());
            let mut missing = false;
            for block_meta in &segment.block_metas {
                if let Some(vector_stats) = block_meta_vector_stats(block_meta, info) {
                    stats.push(vector_stats);
                } else {
                    missing = true;
                }
            }
            (stats, missing)
        })
        .collect::<Vec<_>>();

    for left in 0..segments.len() {
        for right in left + 1..segments.len() {
            if require_scalar_overlap
                && !scalar_cluster_stats_overlap(&segments[left].stats, &segments[right].stats)
            {
                continue;
            }
            let (left_stats, left_missing) = &segment_vector_stats[left];
            let (right_stats, right_missing) = &segment_vector_stats[right];
            let vector_overlap = if *left_missing
                || *right_missing
                || left_stats.is_empty()
                || right_stats.is_empty()
            {
                true
            } else {
                let mut overlap = false;
                'left_stats: for left_stat in left_stats {
                    for right_stat in right_stats {
                        if left_stat.spheres_overlap(right_stat, info.distance_type)? {
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

fn scalar_cluster_stats_overlap(left: &ClusterStatistics, right: &ClusterStatistics) -> bool {
    let left_min = left.min();
    let left_max = left.max();
    let right_min = right.min();
    let right_max = right.max();
    if left_min.len() != left_max.len()
        || left_min.len() != right_min.len()
        || left_min.len() != right_max.len()
    {
        return true;
    }
    left_min
        .iter()
        .zip(left_max.iter())
        .zip(right_min.iter().zip(right_max.iter()))
        .all(|((left_min, left_max), (right_min, right_max))| {
            scalar_le(left_min, right_max) && scalar_le(right_min, left_max)
        })
}

fn scalar_le(
    left: &databend_common_expression::Scalar,
    right: &databend_common_expression::Scalar,
) -> bool {
    matches!(
        left.partial_cmp(right),
        None | Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
    )
}

fn block_meta_vector_stats<'a>(
    block_meta: &'a BlockMeta,
    info: &VectorClusterInfo,
) -> Option<&'a VectorColumnStatistics> {
    block_meta
        .vector_stats
        .as_ref()?
        .get(&(info.column_id, info.distance_type))
}
