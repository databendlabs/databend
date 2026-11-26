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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Scalar;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use indexmap::IndexSet;
use log::debug;

use crate::SegmentLocation;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterMutator;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::passes_depth_gate;
use crate::statistics::RangeMaxTree;

/// Linear cluster-key recluster behavior.
pub(crate) struct LinearReclusterStrategy;

/// Window-local depth state shared by the bounded batch evaluations.
pub(super) struct ReclusterDepthStats<'a> {
    blocks: &'a [&'a ReclusterBlock],
    ranges: Vec<(usize, usize)>,
    range_counts: HashMap<(usize, usize), usize>,
    point_changes: Vec<i64>,
    depth_sum: i128,
}

struct ReclusterOutputRun {
    start: usize,
    end: usize,
    blocks: usize,
}

impl<'a> ReclusterDepthStats<'a> {
    pub(super) fn create(blocks: &'a [&'a ReclusterBlock], key_types: &[DataType]) -> Result<Self> {
        let mut locations = HashSet::with_capacity(blocks.len());
        let mut ranges = Vec::with_capacity(blocks.len());
        for block in blocks {
            let stats = block.stats();
            if stats.min().len() != key_types.len() || stats.max().len() != key_types.len() {
                return Err(ErrorCode::Internal(
                    "Invalid cluster-key arity in recluster window",
                ));
            }
            if !locations.insert(block.meta.location.0.as_str()) {
                return Err(ErrorCode::Internal("Duplicate block in recluster window"));
            }
            ranges.push((stats.min().as_slice(), stats.max().as_slice()));
        }
        let (ranges, points) = index_depth_ranges(&ranges, key_types)?;
        let mut range_counts = HashMap::new();
        for &range in &ranges {
            *range_counts.entry(range).or_insert(0usize) += 1;
        }
        let mut point_changes = vec![0i64; points + 1];
        for (&(start, end), &count) in &range_counts {
            point_changes[start] += count as i64;
            point_changes[end + 1] -= count as i64;
        }
        let depth_sum = if ranges.is_empty() {
            0
        } else {
            let mut depth = 0i64;
            let point_depths = point_changes[..points]
                .iter()
                .map(|change| {
                    depth += change;
                    depth as usize
                })
                .collect::<Vec<_>>();
            let tree = RangeMaxTree::build(&point_depths);
            range_counts
                .iter()
                .map(|(&(start, end), &count)| count as i128 * tree.range_max(start, end) as i128)
                .sum()
        };
        Ok(Self {
            blocks,
            ranges,
            range_counts,
            point_changes,
            depth_sum,
        })
    }

    pub(super) fn gain<'b>(
        &self,
        tasks: impl ExactSizeIterator<Item = &'b [usize]>,
        properties: &ReclusterProperties,
    ) -> Result<i64> {
        let mut selected = HashSet::new();
        let mut outputs = Vec::with_capacity(tasks.len());
        for task in tasks {
            let (mut rows, mut bytes, mut compressed) = (0usize, 0usize, 0usize);
            let (mut start, mut end) = (usize::MAX, 0usize);
            for &block_idx in task {
                let block = self.blocks[block_idx];
                if !selected.insert(block_idx) {
                    return Err(ErrorCode::Internal("Shared input in recluster task batch"));
                }
                start = start.min(self.ranges[block_idx].0);
                end = end.max(self.ranges[block_idx].1);
                rows += block.meta.row_count as usize;
                bytes += block.meta.block_size as usize;
                compressed += block.meta.file_size as usize;
            }
            let blocks =
                estimate_output_block_count(rows, bytes, compressed, &properties.block_thresholds)
                    .ok_or_else(|| ErrorCode::Internal("Empty recluster task candidate"))?;
            outputs.push(ReclusterOutputRun { start, end, blocks });
        }
        if selected.is_empty() {
            return Ok(0);
        }

        let mut removed = HashMap::new();
        for &idx in &selected {
            *removed.entry(self.ranges[idx]).or_insert(0usize) += 1;
        }
        let mut changes = self.point_changes.clone();
        for (&(start, end), &count) in &removed {
            changes[start] -= count as i64;
            changes[end + 1] += count as i64;
        }
        for output in &outputs {
            changes[output.start] += 1;
            changes[output.end + 1] -= 1;
        }
        let mut depth = 0i64;
        let point_depths = changes[..changes.len() - 1]
            .iter()
            .map(|change| {
                depth += change;
                depth as usize
            })
            .collect::<Vec<_>>();
        let tree = RangeMaxTree::build(&point_depths);
        let remaining_depth: i128 = self
            .range_counts
            .iter()
            .map(|(&(start, end), &count)| {
                let count = count - removed.get(&(start, end)).copied().unwrap_or(0);
                count as i128 * tree.range_max(start, end) as i128
            })
            .sum();
        let output_depth: i128 = outputs
            .iter()
            .map(|output| output.blocks as i128 * tree.range_max(output.start, output.end) as i128)
            .sum();
        let blocks_after = self.ranges.len() - selected.len()
            + outputs.iter().map(|output| output.blocks).sum::<usize>();
        let gain = (self.depth_sum * blocks_after as i128
            - (remaining_depth + output_depth) * self.ranges.len() as i128)
            .div_euclid(blocks_after as i128);
        Ok(gain.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }
}

fn estimate_output_block_count(
    rows: usize,
    bytes: usize,
    compressed: usize,
    thresholds: &BlockThresholds,
) -> Option<usize> {
    if rows == 0 || bytes == 0 || compressed == 0 {
        return None;
    }
    let (rows_per_block, _) = thresholds.calc_rows_for_recluster(rows, bytes, compressed);
    Some(rows.div_ceil(rows_per_block))
}

fn index_depth_ranges<T: AsRef<[Scalar]>>(
    ranges: &[(T, T)],
    key_types: &[DataType],
) -> Result<(Vec<(usize, usize)>, usize)> {
    let mut positions = HashMap::new();
    for (min, max) in ranges {
        positions.entry(min.as_ref()).or_insert(0usize);
        positions.entry(max.as_ref()).or_insert(0usize);
    }
    let keys = positions.keys().copied().collect::<Vec<_>>();
    for (pos, idx) in compare_scalars(&keys, key_types)?.into_iter().enumerate() {
        *positions.get_mut(keys[idx as usize]).unwrap() = pos;
    }
    let ranges = ranges
        .iter()
        .map(|(min, max)| (positions[min.as_ref()], positions[max.as_ref()]))
        .collect::<Vec<_>>();
    if ranges.iter().any(|(start, end)| start > end) {
        return Err(ErrorCode::Internal("Invalid recluster block range"));
    }
    Ok((ranges, positions.len()))
}

/// Sweep geometry for bounded multi-peak planning.
pub(super) struct HotspotScan {
    point_depths: Vec<usize>,
    open_pos: Vec<usize>,
    close_pos: Vec<usize>,
    pub(super) average_depth: f64,
    pub(super) peaks: Vec<(usize, usize, usize)>,
}

impl LinearReclusterStrategy {
    pub(super) fn scan_hotspots(
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
    ) -> Option<HotspotScan> {
        let mut points_map = BTreeMap::new();
        for (local_idx, &idx) in indices.iter().enumerate() {
            let stats = blocks[idx].stats();
            let (min, max) = (stats.min().as_slice(), stats.max().as_slice());
            if min.len() != properties.scalar_cluster_key_types.len()
                || max.len() != properties.scalar_cluster_key_types.len()
            {
                continue;
            }
            let point: &mut (Vec<usize>, Vec<usize>) =
                points_map.entry(ScalarSlice(min)).or_default();
            point.0.push(local_idx);
            points_map
                .entry(ScalarSlice(max))
                .or_default()
                .1
                .push(local_idx);
        }
        if points_map.is_empty() {
            return None;
        }

        let values = points_map.into_values().collect::<Vec<_>>();
        let mut point_depths = vec![0usize; values.len()];
        let mut open_pos = vec![usize::MAX; indices.len()];
        let mut close_pos = vec![usize::MAX; indices.len()];
        let mut live = vec![false; indices.len()];
        let mut live_count = 0usize;
        let mut max_depth = 0usize;
        let mut peaks = Vec::new();
        let mut current_peak: Option<(usize, usize, usize)> = None;
        for (pos, (starts, ends)) in values.iter().enumerate() {
            let depth = calc_point_depth(live_count, starts, ends);
            point_depths[pos] = depth;
            max_depth = max_depth.max(depth);
            if depth as f64 > properties.depth_threshold {
                match &mut current_peak {
                    Some((peak_pos, peak_depth, width)) => {
                        *width += 1;
                        if depth > *peak_depth {
                            *peak_pos = pos;
                            *peak_depth = depth;
                        }
                    }
                    None => current_peak = Some((pos, depth, 1)),
                }
            } else if let Some(peak) = current_peak.take() {
                peaks.push(peak);
            }
            for &idx in starts {
                if !live[idx] {
                    live[idx] = true;
                    live_count += 1;
                }
                open_pos[idx] = pos;
            }
            for &idx in ends {
                if live[idx] {
                    live[idx] = false;
                    live_count -= 1;
                    close_pos[idx] = pos;
                }
            }
        }
        if let Some(peak) = current_peak {
            peaks.push(peak);
        }

        let tree = RangeMaxTree::build(&point_depths);
        let mut sum_depth = 0usize;
        let mut closed = 0usize;
        for (&open, &close) in open_pos.iter().zip(&close_pos) {
            if open == usize::MAX {
                continue;
            }
            if close == usize::MAX || close < open {
                debug!(
                    "recluster: candidate selection detail group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=invalid_depth_range",
                    group,
                    indices.len(),
                    f64::NAN,
                    max_depth,
                );
                return None;
            }
            sum_depth += tree.range_max(open, close);
            closed += 1;
        }
        if closed == 0 {
            return None;
        }
        let average_depth = (10000.0 * sum_depth as f64 / closed as f64).round() / 10000.0;
        if !passes_depth_gate(properties.depth_threshold, average_depth, max_depth) {
            debug!(
                "recluster: candidate selection detail group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=below_hotspot_depth_gate",
                group,
                indices.len(),
                average_depth,
                max_depth,
            );
            return None;
        }
        peaks.sort_by(
            |(left_pos, left_depth, left_width), (right_pos, right_depth, right_width)| {
                right_depth
                    .cmp(left_depth)
                    .then_with(|| right_width.cmp(left_width))
                    .then_with(|| left_pos.cmp(right_pos))
            },
        );
        Some(HotspotScan {
            point_depths,
            open_pos,
            close_pos,
            average_depth,
            peaks,
        })
    }
}

impl HotspotScan {
    /// Build at most `limit` tasks around one peak. Skip-fill preserves the
    /// multi-peak exploration; contiguous runs compete under the same gain score.
    pub(super) fn peak_packs(
        &self,
        peak: usize,
        centered: bool,
        contiguous: bool,
        limit: usize,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        budget: usize,
    ) -> Vec<(Vec<usize>, usize)> {
        let peak_depth = self.point_depths[peak];
        let mut left = peak;
        while left > 0 && self.point_depths[left - 1] == peak_depth {
            left -= 1;
        }
        let mut right = peak;
        while right + 1 < self.point_depths.len() && self.point_depths[right + 1] == peak_depth {
            right += 1;
        }
        let mut order = (0..indices.len())
            .filter(|&idx| self.open_pos[idx] <= right && self.close_pos[idx] >= left)
            .collect::<Vec<_>>();
        if centered {
            order.sort_by_key(|&idx| {
                (
                    self.open_pos[idx]
                        .abs_diff(peak)
                        .max(self.close_pos[idx].abs_diff(peak)),
                    idx,
                )
            });
        }
        let order = order
            .into_iter()
            .map(|local_idx| {
                let idx = indices[local_idx];
                (idx, blocks[idx].meta.block_size as usize)
            })
            .collect();
        pack_peak_order(order, contiguous, limit, budget)
    }
}

fn pack_peak_order(
    mut order: Vec<(usize, usize)>,
    contiguous: bool,
    limit: usize,
    budget: usize,
) -> Vec<(Vec<usize>, usize)> {
    order.retain(|&(_, size)| size <= budget);
    let mut tasks = Vec::with_capacity(limit);
    let mut selected = Vec::new();
    if !contiguous {
        while tasks.len() < limit && !order.is_empty() {
            selected.clear();
            let mut bytes = 0usize;
            order.retain(|&(idx, size)| {
                if size <= budget - bytes {
                    selected.push(idx);
                    bytes += size;
                    false
                } else {
                    true
                }
            });
            if selected.len() >= 2 {
                selected.sort_unstable();
                tasks.push((selected.clone(), bytes));
            }
        }
        return tasks;
    }

    let mut bytes = 0usize;
    for (idx, size) in order {
        if !selected.is_empty() && bytes.saturating_add(size) > budget {
            if selected.len() >= 2 {
                selected.sort_unstable();
                tasks.push((std::mem::take(&mut selected), bytes));
                if tasks.len() >= limit {
                    return tasks;
                }
            } else {
                selected.clear();
            }
            bytes = 0;
        }
        selected.push(idx);
        bytes += size;
    }
    if selected.len() >= 2 && tasks.len() < limit {
        selected.sort_unstable();
        tasks.push((selected, bytes));
    }
    tasks
}

impl ReclusterStrategy for LinearReclusterStrategy {
    fn supports_ordered_merge(&self) -> bool {
        true
    }

    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        select_scalar_segments(self, properties, compact_segments, window_len)
    }

    fn fetch_task_candidates(
        &self,
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        // Compact/sort preparation may leave only an ordered subset of the
        // window. Score that subset with the same joint-gain selector, never
        // with a second overlap algorithm. Block indices remain source-local.
        let group_blocks = indices.iter().map(|&idx| blocks[idx]).collect::<Vec<_>>();
        ReclusterMutator::plan_linear_tasks(
            properties,
            &group_blocks,
            [(group, (0..group_blocks.len()).collect())],
            None,
            task_budget,
        )
    }

    fn can_reuse_cluster_stats(
        &self,
        properties: &ReclusterProperties,
        stats: &ClusterStatistics,
    ) -> bool {
        stats.cluster_key_id == properties.cluster_key_info.cluster_key_id()
            && stats.min().len() == properties.scalar_cluster_key_types.len()
            && stats.max().len() == properties.scalar_cluster_key_types.len()
    }
}

/// Select scalar segment windows with a sweep over segment min/max points.
pub(crate) fn select_scalar_segments(
    strategy: &dyn ReclusterStrategy,
    properties: &ReclusterProperties,
    compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
    window_len: usize,
) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
    let window_len = window_len.max(1);
    let block_per_seg = properties.block_thresholds.block_per_segment;

    let mut total_blocks = 0;
    let mut segments = vec![None; compact_segments.len()];
    let mut segment_stats = Vec::with_capacity(compact_segments.len());

    // Phase 1: collect segment ranges for the sweep-line selection. Large
    // unclustered segments are skipped because rewriting them is not useful.
    for (i, (loc, compact_segment)) in compact_segments.iter().enumerate() {
        let stats = strategy.build_cluster_stats(
            properties,
            compact_segment.summary.cluster_stats.as_ref(),
            &compact_segment.summary.col_stats,
            compact_segment.summary.row_count,
        );
        let level = stats.level;

        if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
            continue;
        }

        total_blocks += compact_segment.summary.block_count as usize;
        let (min, max) = (stats.min().as_slice(), stats.max().as_slice());
        if min.len() != properties.scalar_cluster_key_types.len()
            || max.len() != properties.scalar_cluster_key_types.len()
        {
            continue;
        }
        segment_stats.push((i, stats.min, stats.max));
        segments[i] = Some(SelectedReclusterSegment {
            loc: loc.clone(),
            info: compact_segment.clone(),
        });
    }

    let mut windows: Vec<(IndexSet<usize>, usize)> = Vec::new();

    // Phase 2: sweep the cluster-key points and cut the candidate segments
    // into consecutive, segment-disjoint windows. Each segment joins exactly
    // one window (at its start point), so windows never share a segment and
    // tasks never read the same block twice. A window closes at `window_len`;
    // `prev_window` holds the last closed one so a small tail can fold into it.
    let mut unfinished_intervals = BTreeMap::new();
    let mut prev_window: Option<(IndexSet<usize>, usize)> = None;
    let mut current_window: IndexSet<usize> = IndexSet::new();
    let mut current_window_max_depth = 0usize;
    let mut segment_points = BTreeMap::new();
    for (i, min, max) in &segment_stats {
        let point: &mut (Vec<usize>, Vec<usize>) =
            segment_points.entry(ScalarSlice(min)).or_default();
        point.0.push(*i);
        let point = segment_points.entry(ScalarSlice(max)).or_default();
        point.1.push(*i);
    }

    for (_, (start, end)) in segment_points {
        let point_depth = calc_point_depth(unfinished_intervals.len(), &start, &end);

        // A window is just a contiguous run of segments, so partitioning the
        // run keeps windows segment-disjoint without any extra bookkeeping.
        // Depth only contributes to the window score.
        current_window_max_depth = current_window_max_depth.max(point_depth);
        current_window.extend(start.iter().copied());

        if current_window.len() >= window_len {
            // Emit the previously closed window and rotate the current
            // window into `prev_window`.
            if let Some((segs, depth)) = prev_window.take() {
                windows.push((segs, depth));
            }
            prev_window = Some((
                std::mem::take(&mut current_window),
                current_window_max_depth,
            ));
            current_window_max_depth = 0;
        }

        start.iter().for_each(|&idx| {
            unfinished_intervals.insert(idx, point_depth);
        });
        end.iter().for_each(|idx| {
            unfinished_intervals.remove(idx);
        });
    }

    // Fold the trailing window into the last closed one to avoid a tiny
    // fragment; this may push it past `window_len` (an acceptable soft
    // overshoot under `LIMIT`).
    if let Some((mut prev_segs, prev_depth)) = prev_window.take() {
        prev_segs.extend(current_window);
        windows.push((prev_segs, prev_depth.max(current_window_max_depth)));
    } else if !current_window.is_empty() {
        windows.push((current_window, current_window_max_depth));
    }

    // Try the deepest windows first; for equal depth, prefer the larger
    // window because it gives candidate probing more room to build tasks.
    windows.sort_by(|(left_indices, left_depth), (right_indices, right_depth)| {
        right_depth
            .cmp(left_depth)
            .then_with(|| right_indices.len().cmp(&left_indices.len()))
    });

    if properties.mode == ReclusterMode::Conservative {
        // Conservative mode is kept for legacy tables without
        // `aggressive_recluster`. Probe only the deepest window per round to
        // avoid over-reclustering old tables and creating a sharp behavior
        // gap from the pre-option strategy.
        windows.truncate(1);
    }

    debug!(
        "recluster: segment selection windows segments={} blocks={} window_count={}",
        compact_segments.len(),
        total_blocks,
        windows.len(),
    );

    // Convert index windows back to segment objects.
    Ok(windows
        .into_iter()
        .map(|(selected_indices, _)| {
            selected_indices
                .into_iter()
                .filter_map(|i| segments[i].clone())
                .collect::<Vec<_>>()
        })
        .filter(|window| !window.is_empty())
        .collect())
}

#[derive(Clone, Copy)]
struct ScalarSlice<'a>(&'a [Scalar]);

impl Ord for ScalarSlice<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .iter()
            .map(Scalar::as_ref)
            .cmp(other.0.iter().map(Scalar::as_ref))
    }
}

impl PartialOrd for ScalarSlice<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScalarSlice<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ScalarSlice<'_> {}

fn calc_point_depth(open_interval_count: usize, start: &[usize], end: &[usize]) -> usize {
    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    if open_interval_count == 1
        && !start.is_empty()
        && !end.is_empty()
        && start.len() + end.len() <= 3
    {
        let set: HashSet<usize> = HashSet::from_iter(start.iter().chain(end.iter()).cloned());
        if set.len() == 2 {
            return 1;
        }
    }

    open_interval_count + start.len()
}

#[cfg(test)]
mod tests {
    use super::pack_peak_order;

    #[test]
    fn test_peak_packing_switches_from_skip_fill_to_contiguous() {
        let order = vec![(0, 6), (1, 6), (2, 4), (3, 4)];

        assert_eq!(pack_peak_order(order.clone(), false, 2, 10), vec![
            (vec![0, 2], 10),
            (vec![1, 3], 10)
        ]);
        assert_eq!(pack_peak_order(order, true, 2, 10), vec![(vec![1, 2], 10)]);
    }
}
