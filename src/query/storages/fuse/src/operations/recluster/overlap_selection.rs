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

use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use indexmap::IndexSet;
use log::debug;

use super::recluster_mutator::CandidateScore;
use super::recluster_mutator::ReclusterBlock;
use super::recluster_mutator::ReclusterGroup;
use super::recluster_mutator::ReclusterMutator;
use super::recluster_mutator::ReclusterTaskCandidate;
use super::recluster_mutator::SelectedReclusterSegment;
use crate::SegmentLocation;

/// Strategy hook for building block tasks and segment windows from overlap candidates.
pub(super) trait OverlapSelector {
    const LABEL: &'static str;

    fn depth_threshold(&self, default_threshold: f64) -> f64 {
        default_threshold
    }

    fn depth_summary(&self) -> (usize, usize);

    fn next_seed(&mut self, used: &[bool]) -> Option<(usize, usize)>;

    fn members_by_priority(&self, seed: usize, used: &[bool]) -> Vec<usize>;

    fn block_seed(&mut self, _seed: usize) {}

    fn next_window(
        &mut self,
        window_len: usize,
        seen_windows: &mut HashSet<Vec<usize>>,
        covered_segments: &[bool],
    ) -> Option<Vec<usize>> {
        while let Some((seed, _)) = self.next_seed(covered_segments) {
            let mut selected_indices = self.members_by_priority(seed, covered_segments);
            selected_indices.truncate(window_len);
            if selected_indices.len() < 2 {
                self.block_seed(seed);
                continue;
            }

            let mut window_key = selected_indices.clone();
            window_key.sort_unstable();
            if seen_windows.insert(window_key) {
                return Some(selected_indices);
            }

            self.block_seed(seed);
        }
        None
    }
}

/// Segment wrapper used by vector/Hilbert overlap refinement.
pub(super) trait OverlapSegmentItem: Sized {
    fn from_selected_segment(
        segment: SelectedReclusterSegment,
        stats: ClusterStatistics,
    ) -> Result<Self>;

    fn selected_segment(&self) -> &SelectedReclusterSegment;
}

/// Initialize each item as overlapping itself.
pub(super) fn identity_overlaps(len: usize) -> Vec<IndexSet<usize>> {
    let mut overlaps = vec![IndexSet::new(); len];
    for (idx, overlap) in overlaps.iter_mut().enumerate() {
        overlap.insert(idx);
    }
    overlaps
}

/// Order candidates by overlap degree, deepest first.
pub(super) fn overlap_depth_order(overlaps: &[IndexSet<usize>]) -> Vec<(usize, usize)> {
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
    depth_order
}

/// Convert scalar-selected segments into strategy-specific overlap items.
pub(super) fn collect_overlap_window_segments<T>(
    mutator: &ReclusterMutator,
    scalar_window: Vec<SelectedReclusterSegment>,
) -> Result<Vec<T>>
where
    T: OverlapSegmentItem,
{
    let mut items = Vec::with_capacity(scalar_window.len());
    for segment in scalar_window {
        let stats = mutator.build_cluster_stats_for_recluster(
            segment.info.summary.cluster_stats.as_ref(),
            &segment.info.summary.col_stats,
        );
        items.push(T::from_selected_segment(segment, stats)?);
    }
    Ok(items)
}

impl ReclusterMutator {
    /// Build task candidates by repeatedly picking the deepest unused overlap seed.
    pub(super) fn fetch_overlap_task_candidates<T>(
        &self,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        mut selector: T,
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>>
    where
        T: OverlapSelector,
    {
        let block_count = indices.len();
        if block_count < 2 {
            return Ok(Vec::new());
        }

        let (max_depth, sum_depth) = selector.depth_summary();
        let average_depth = (10000.0 * sum_depth as f64 / block_count as f64).round() / 10000.0;
        let depth_threshold = selector.depth_threshold(self.depth_threshold);
        if !Self::passes_depth_gate(depth_threshold, average_depth, max_depth) {
            debug!(
                "recluster: {} candidate selection group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=below_{}_depth_gate",
                T::LABEL,
                group,
                block_count,
                average_depth,
                max_depth,
                T::LABEL,
            );
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        let mut used_blocks = vec![false; block_count];
        while candidates.len() < task_budget {
            let Some((seed, depth)) = selector.next_seed(&used_blocks) else {
                break;
            };

            let local_indices = selector.members_by_priority(seed, &used_blocks);
            if local_indices.len() < 2 {
                selector.block_seed(seed);
                continue;
            }

            let mut task_bytes = 0usize;
            let mut selected_local_indices = Vec::new();
            for local_idx in local_indices {
                let block_size = blocks[indices[local_idx]].meta.block_size as usize;
                if !selected_local_indices.is_empty()
                    && task_bytes.saturating_add(block_size) > self.memory_threshold
                {
                    break;
                }
                task_bytes = task_bytes.saturating_add(block_size);
                selected_local_indices.push(local_idx);
            }
            if selected_local_indices.len() < 2 {
                selector.block_seed(seed);
                continue;
            }

            for &local_idx in &selected_local_indices {
                used_blocks[local_idx] = true;
            }
            let task_indices = selected_local_indices
                .into_iter()
                .map(|local_idx| indices[local_idx])
                .collect::<Vec<_>>();
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                max_depth: depth,
                average_depth,
            };
            candidates.push(Self::task_candidate(group, score, &task_indices, blocks));
        }

        debug!(
            "recluster: {} candidate selection group={} block_count={} avg_depth={} depth_threshold={} max_depth={} task_count={}",
            T::LABEL,
            group,
            block_count,
            average_depth,
            depth_threshold,
            max_depth,
            candidates.len(),
        );

        Ok(candidates)
    }
}

/// Select segment windows directly from vector/Hilbert overlap without scalar keys.
pub(super) fn select_overlap_only_segments<T, P, F>(
    mutator: &ReclusterMutator,
    compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
    window_len: usize,
    make_picker: F,
) -> Result<Vec<Vec<SelectedReclusterSegment>>>
where
    T: OverlapSegmentItem,
    P: OverlapSelector,
    F: FnOnce(&[T]) -> Result<P>,
{
    let window_len = window_len.max(1);
    let block_per_seg = mutator.block_thresholds.block_per_segment;
    let mut total_blocks = 0;
    let mut items = Vec::with_capacity(compact_segments.len());

    for (loc, compact_segment) in compact_segments {
        let current_blocks_num = compact_segment.summary.block_count as usize;
        let stats = mutator.build_cluster_stats_for_recluster(
            compact_segment.summary.cluster_stats.as_ref(),
            &compact_segment.summary.col_stats,
        );
        if stats.level < 0 && current_blocks_num >= block_per_seg {
            continue;
        }

        total_blocks += current_blocks_num;
        items.push(T::from_selected_segment(
            SelectedReclusterSegment {
                loc: loc.clone(),
                info: compact_segment.clone(),
            },
            stats,
        )?);
    }

    let mut windows = Vec::new();
    let mut seen_windows = HashSet::new();
    let mut covered_segments = vec![false; items.len()];

    if items.len() > 1 && total_blocks > block_per_seg {
        let mut picker = make_picker(&items)?;
        while let Some(selected_indices) =
            picker.next_window(window_len, &mut seen_windows, &covered_segments)
        {
            for idx in &selected_indices {
                covered_segments[*idx] = true;
            }
            windows.push(selected_indices);
        }
        let covered_count = covered_segments.iter().filter(|covered| **covered).count();

        debug!(
            "recluster: {} segment selection overlap windows segments={} blocks={} window_count={} covered_segments={}",
            P::LABEL,
            items.len(),
            total_blocks,
            windows.len(),
            covered_count,
        );
    }

    let mut fallback_window = Vec::with_capacity(window_len);
    for (idx, covered) in covered_segments.iter().enumerate() {
        if *covered {
            continue;
        }

        fallback_window.push(idx);
        if fallback_window.len() == window_len {
            if seen_windows.insert(fallback_window.clone()) {
                windows.push(std::mem::replace(
                    &mut fallback_window,
                    Vec::with_capacity(window_len),
                ));
            } else {
                fallback_window.clear();
            }
        }
    }
    if !fallback_window.is_empty() && seen_windows.insert(fallback_window.clone()) {
        windows.push(fallback_window);
    }

    Ok(windows
        .into_iter()
        .map(|selected_indices| {
            selected_indices
                .into_iter()
                .map(|i| items[i].selected_segment().clone())
                .collect::<Vec<_>>()
        })
        .filter(|window| !window.is_empty())
        .collect())
}

/// Refine scalar-selected windows by splitting them into overlap components.
pub(super) fn refine_scalar_windows_by_overlap<T, B>(
    scalar_windows: Vec<Vec<SelectedReclusterSegment>>,
    window_len: usize,
    mut build: B,
) -> Result<Vec<Vec<SelectedReclusterSegment>>>
where
    T: OverlapSegmentItem,
    B: FnMut(Vec<SelectedReclusterSegment>) -> Result<(Vec<T>, Vec<IndexSet<usize>>)>,
{
    let window_len = window_len.max(1);
    let mut refined_windows = Vec::with_capacity(scalar_windows.len());
    let mut seen_windows = HashSet::new();

    for scalar_window in scalar_windows {
        if scalar_window.len() < 2 {
            let window_key = scalar_window
                .iter()
                .map(|segment| segment.loc.segment_idx)
                .collect::<Vec<_>>();
            if seen_windows.insert(window_key) {
                refined_windows.push(scalar_window);
            }
            continue;
        }

        let (items, overlaps) = build(scalar_window)?;
        let mut visited = vec![false; items.len()];
        for start in 0..items.len() {
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

            component.sort_unstable_by_key(|idx| items[*idx].selected_segment().loc.segment_idx);
            for chunk in component.chunks(window_len) {
                let window_key = chunk
                    .iter()
                    .map(|idx| items[*idx].selected_segment().loc.segment_idx)
                    .collect::<Vec<_>>();
                if seen_windows.insert(window_key) {
                    let window = chunk
                        .iter()
                        .map(|idx| items[*idx].selected_segment().clone())
                        .collect::<Vec<_>>();
                    refined_windows.push(window);
                }
            }
        }
    }

    Ok(refined_windows)
}
