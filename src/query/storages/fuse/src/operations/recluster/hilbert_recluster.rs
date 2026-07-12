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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_sql::HILBERT_CLUSTER_DIMENSIONS;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::valid_cluster_stats_hilbert_minmax;
use log::debug;

use crate::SegmentLocation;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::task_candidate;

/// Hilbert cluster-key recluster behavior.
pub(crate) struct HilbertReclusterStrategy;

impl ReclusterStrategy for HilbertReclusterStrategy {
    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        soft_min_segments: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        let block_per_segment = properties.block_thresholds.block_per_segment;
        let mut segments = Vec::with_capacity(compact_segments.len());
        let mut bounds = Vec::with_capacity(compact_segments.len());
        let mut total_blocks = 0usize;

        // ReclusterMutator groups segments by partition before dispatching to a strategy. This
        // method therefore only selects Hilbert MBR windows within one partition-local slice.
        for (loc, info) in compact_segments {
            let stats = self.build_cluster_stats(
                properties,
                info.summary.cluster_stats.as_ref(),
                &info.summary.col_stats,
            );
            let block_count = info.summary.block_count as usize;
            if stats.level < 0 && block_count >= block_per_segment {
                continue;
            }
            total_blocks += block_count;
            segments.push(SelectedReclusterSegment {
                loc: loc.clone(),
                info: info.clone(),
            });
            bounds.push(Rect::try_from_stats(&stats)?);
        }

        let soft_min_segments = soft_min_segments.max(1).min(segments.len().max(1));
        let mut assigned = vec![false; bounds.len()];
        let mut owners = vec![None; bounds.len()];
        let mut open: Vec<Option<Vec<usize>>> = Vec::new();
        let mut windows = Vec::new();

        // Keep related hotspots open only until they reach the soft minimum. Closed windows are not
        // extended again, which bounds transitive growth without truncating one large hotspot.
        // Retain two alternatives per expected output window while keeping refinement bounded.
        let expected_window_count = bounds.len().div_ceil(soft_min_segments);
        let boxes = rank_rects(&bounds);
        for witness in top_witnesses(&boxes, witness_budget(expected_window_count)) {
            let members = members_at_witness(&boxes, witness)
                .into_iter()
                .filter(|idx| !assigned[*idx])
                .collect::<Vec<_>>();
            if members.len() < 2 {
                continue;
            }

            let mut matches = members
                .iter()
                .filter_map(|idx| owners[*idx])
                .collect::<Vec<_>>();
            matches.sort_unstable();
            matches.dedup();
            let owner = matches.first().copied().unwrap_or_else(|| {
                open.push(None);
                open.len() - 1
            });
            let mut window = open[owner].take().unwrap_or_default();
            for other in matches.into_iter().skip(1) {
                if let Some(members) = open[other].take() {
                    window.extend(members);
                }
            }
            window.extend(members);
            window.sort_unstable();
            window.dedup();

            if window.len() >= soft_min_segments {
                for &idx in &window {
                    assigned[idx] = true;
                    owners[idx] = None;
                }
                windows.push(window);
            } else {
                for &idx in &window {
                    owners[idx] = Some(owner);
                }
                open[owner] = Some(window);
            }
        }

        for window in open.into_iter().flatten() {
            for &idx in &window {
                assigned[idx] = true;
            }
            windows.push(window);
        }
        let leftovers = assigned
            .iter()
            .enumerate()
            .filter_map(|(idx, assigned)| (!assigned).then_some(idx))
            .collect::<Vec<_>>();
        windows.extend(
            leftovers
                .chunks(soft_min_segments)
                .map(|chunk| chunk.to_vec()),
        );

        let selected_windows = windows
            .into_iter()
            .map(|window| {
                window
                    .into_iter()
                    .map(|idx| segments[idx].clone())
                    .collect()
            })
            .collect::<Vec<_>>();

        debug!(
            "recluster: hilbert segment selection segments={} blocks={} windows={} soft_min={}",
            segments.len(),
            total_blocks,
            selected_windows.len(),
            soft_min_segments,
        );
        Ok(selected_windows)
    }

    fn fetch_task_candidates(
        &self,
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        if indices.len() < 2 || task_budget == 0 {
            return Ok(Vec::new());
        }

        let mut ordered_indices = indices.to_vec();
        ordered_indices.sort_by_key(|idx| (blocks[*idx].meta.block_size, *idx));
        let bounds = ordered_indices
            .iter()
            .map(|idx| Rect::try_from_stats(blocks[*idx].stats()))
            .collect::<Result<Vec<_>>>()?;
        let boxes = rank_rects(&bounds);
        let witnesses = top_witnesses(&boxes, witness_budget(task_budget));
        let block_size =
            |local_idx: usize| blocks[ordered_indices[local_idx]].meta.block_size as usize;
        let mut used = vec![false; indices.len()];
        let mut tasks = Vec::new();

        for witness in witnesses {
            if tasks.len() == task_budget {
                break;
            }
            let members = members_at_witness(&boxes, witness)
                .into_iter()
                .filter(|idx| !used[*idx])
                .collect::<Vec<_>>();
            if members.len() as f64 <= properties.depth_threshold {
                continue;
            }

            let mut cursor = 0usize;
            while tasks.len() < task_budget {
                // Growth below may consume members of this same witness, so skip anything an
                // earlier task already took.
                let mut selected = Vec::new();
                let mut task_bytes = 0usize;
                while cursor < members.len() {
                    let candidate = members[cursor];
                    if used[candidate] {
                        cursor += 1;
                        continue;
                    }
                    // Keep at least two blocks even when the pair exceeds the soft memory limit.
                    let size = block_size(candidate);
                    if selected.len() >= 2
                        && task_bytes.saturating_add(size) > properties.memory_threshold
                    {
                        break;
                    }
                    task_bytes = task_bytes.saturating_add(size);
                    selected.push(candidate);
                    cursor += 1;
                }
                if selected.len() < 2 {
                    break;
                }

                // Every selected block contains this hotspot's exact XY witness, so the hotspot
                // depth is exactly the number of blocks picked from the witness members. Growth
                // below adds overlapping blocks without deepening this hotspot, so the reported
                // depth must stay pinned to the core count.
                let core_depth = selected.len();
                if core_depth as f64 <= properties.depth_threshold {
                    continue;
                }
                for &local_idx in &selected {
                    used[local_idx] = true;
                }

                // A witness caps a task at the hotspot depth, which is unrelated to the memory
                // budget: a depth-17 hotspot yields a ~35MB task even when the budget is several
                // GB, so RECLUSTER FINAL needs hundreds of near-empty rounds to converge. Absorb
                // additional unused blocks that overlap the core's fixed bounding rectangle so
                // one rewrite removes proportionally more local overlap without drifting across
                // the window while it grows.
                if task_bytes < properties.memory_threshold {
                    let core = Rect::bounding(&boxes, &selected);
                    // `ordered_indices` is sorted by ascending block size, so the first block that
                    // does not fit ends the scan.
                    for local_idx in 0..boxes.len() {
                        if used[local_idx] || !core.intersects(&boxes[local_idx]) {
                            continue;
                        }
                        let size = block_size(local_idx);
                        if task_bytes.saturating_add(size) > properties.memory_threshold {
                            break;
                        }
                        task_bytes = task_bytes.saturating_add(size);
                        selected.push(local_idx);
                        used[local_idx] = true;
                    }
                }

                let task_indices = selected
                    .into_iter()
                    .map(|local_idx| ordered_indices[local_idx])
                    .collect::<Vec<_>>();
                tasks.push(task_candidate(
                    group,
                    CandidateScore {
                        selected_total_bytes: task_bytes,
                        max_depth: core_depth,
                        average_depth: core_depth as f64,
                    },
                    &task_indices,
                    blocks,
                ));
            }
        }

        debug!(
            "recluster: hilbert task selection group={} blocks={} tasks={}",
            group,
            indices.len(),
            tasks.len(),
        );
        Ok(tasks)
    }

    fn can_reuse_cluster_stats(
        &self,
        properties: &ReclusterProperties,
        stats: &ClusterStatistics,
    ) -> bool {
        stats.cluster_key_id == properties.cluster_key_id
            && stats.min().len() == properties.prepared_cluster_key_exprs.len()
            && stats.max().len() == properties.prepared_cluster_key_exprs.len()
            && valid_cluster_stats_hilbert_minmax(stats, HILBERT_CLUSTER_DIMENSIONS).is_some()
    }
}

/// Retain two witness alternatives per target and cap refinement at 1024 witnesses.
/// With B retained witnesses, discovery costs O(n log n + n log B + Bn) time and
/// O(n + B) memory.
fn witness_budget(target_count: usize) -> usize {
    target_count.saturating_mul(2).min(1024)
}

#[derive(Clone, Copy, Default)]
struct Rect<T> {
    x_min: T,
    x_max: T,
    y_min: T,
    y_max: T,
}

impl Rect<usize> {
    /// Minimum bounding rectangle of the given members.
    fn bounding(boxes: &[Rect<usize>], members: &[usize]) -> Self {
        let mut iter = members.iter();
        let first = iter
            .next()
            .map(|idx| boxes[*idx])
            .expect("bounding rectangle requires at least one member");
        iter.fold(first, |acc, idx| {
            let item = &boxes[*idx];
            Rect {
                x_min: acc.x_min.min(item.x_min),
                x_max: acc.x_max.max(item.x_max),
                y_min: acc.y_min.min(item.y_min),
                y_max: acc.y_max.max(item.y_max),
            }
        })
    }

    /// Whether two closed rectangles share at least one point.
    fn intersects(&self, other: &Self) -> bool {
        self.x_min <= other.x_max
            && other.x_min <= self.x_max
            && self.y_min <= other.y_max
            && other.y_min <= self.y_max
    }
}

impl Rect<Scalar> {
    fn try_from_stats(stats: &ClusterStatistics) -> Result<Self> {
        let (min, max) = valid_cluster_stats_hilbert_minmax(stats, HILBERT_CLUSTER_DIMENSIONS)
            .ok_or_else(|| {
                ErrorCode::Internal("Hilbert overlap requires normalized 2D cluster statistics")
            })?;
        Ok(Self {
            x_min: min[0].clone(),
            x_max: max[0].clone(),
            y_min: min[1].clone(),
            y_max: max[1].clone(),
        })
    }
}

/// A point covered by a set of Hilbert MBRs.
///
/// Ordering is by quality: deeper witnesses are better, followed by stable coordinate tie-breaks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Witness {
    x: usize,
    y: usize,
    depth: usize,
    signature: [u64; 2],
}

impl Ord for Witness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.depth
            .cmp(&other.depth)
            .then_with(|| other.x.cmp(&self.x))
            .then_with(|| other.y.cmp(&self.y))
            .then_with(|| self.signature.cmp(&other.signature))
    }
}

impl PartialOrd for Witness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Retains the best witness for each active-set fingerprint and enforces a hard memory bound.
///
/// The double fingerprint only improves candidate diversity. Exact depth comes from the range tree,
/// and candidate membership is always reconstructed from the original rectangles before planning.
struct WitnessCollector {
    budget: usize,
    witnesses: BTreeSet<Witness>,
    by_signature: HashMap<[u64; 2], Witness>,
}

impl WitnessCollector {
    fn new(budget: usize) -> Self {
        Self {
            budget,
            witnesses: BTreeSet::new(),
            by_signature: HashMap::with_capacity(budget),
        }
    }

    fn push(&mut self, witness: Witness) {
        if self.budget == 0 || witness.depth < 2 {
            return;
        }
        if let Some(current) = self.by_signature.get(&witness.signature).copied() {
            if witness <= current {
                return;
            }
            self.witnesses.remove(&current);
        }
        self.witnesses.insert(witness);
        self.by_signature.insert(witness.signature, witness);

        if self.witnesses.len() > self.budget {
            let worst = *self
                .witnesses
                .first()
                .expect("collector exceeded a non-zero witness budget");
            self.witnesses.remove(&worst);
            self.by_signature.remove(&worst.signature);
        }
    }
}

/// Find diverse high-depth witnesses without materializing their rectangle member lists.
fn top_witnesses(boxes: &[Rect<usize>], budget: usize) -> Vec<Witness> {
    if boxes.len() < 2 || budget == 0 {
        return Vec::new();
    }
    let mut collector = WitnessCollector::new(budget);
    sweep_xy(boxes, false, |witness| collector.push(witness));
    sweep_xy(boxes, true, |witness| collector.push(witness));
    collector.witnesses.into_iter().rev().collect()
}

/// Materialize one candidate only when the planner is ready to consume it.
fn members_at_witness(boxes: &[Rect<usize>], witness: Witness) -> Vec<usize> {
    boxes
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            (item.x_min <= witness.x
                && witness.x <= item.x_max
                && item.y_min <= witness.y
                && witness.y <= item.y_max)
                .then_some(idx)
        })
        .collect()
}

fn sweep_xy<F>(boxes: &[Rect<usize>], transpose: bool, mut visit: F)
where F: FnMut(Witness) {
    let projected = boxes
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let (primary_min, primary_max, secondary_min, secondary_max) = if transpose {
                (item.y_min, item.y_max, item.x_min, item.x_max)
            } else {
                (item.x_min, item.x_max, item.y_min, item.y_max)
            };
            (
                primary_min,
                primary_max,
                secondary_min,
                secondary_max,
                rectangle_signature(idx),
            )
        })
        .collect::<Vec<_>>();
    let primary_count = projected
        .iter()
        .map(|(_, max, _, _, _)| *max)
        .max()
        .unwrap_or(0)
        + 1;
    let secondary_count = projected
        .iter()
        .map(|(_, _, _, max, _)| *max)
        .max()
        .unwrap_or(0)
        + 1;
    let mut starts = projected
        .iter()
        .map(|(min, _, secondary_min, secondary_max, signature)| {
            (*min, *secondary_min, *secondary_max, *signature)
        })
        .collect::<Vec<_>>();
    let mut ends = projected
        .iter()
        .map(|(_, max, secondary_min, secondary_max, signature)| {
            (*max, *secondary_min, *secondary_max, *signature)
        })
        .collect::<Vec<_>>();
    starts.sort_unstable();
    ends.sort_unstable();

    let mut tree = RangeAddMaxTree::new(secondary_count);
    let mut start_pos = 0usize;
    let mut end_pos = 0usize;
    for primary in 0..primary_count {
        while start_pos < starts.len() && starts[start_pos].0 == primary {
            let (_, min, max, signature) = starts[start_pos];
            tree.add_with_signature(min, max + 1, 1, signature);
            start_pos += 1;
        }
        let depth = tree.max();
        if depth >= 2 {
            let (secondary, signature) = tree.argmax();
            let (x, y) = if transpose {
                (secondary, primary)
            } else {
                (primary, secondary)
            };
            visit(Witness {
                x,
                y,
                depth,
                signature,
            });
        }
        // Starts are applied before ends so touching closed MBRs overlap at their boundary.
        while end_pos < ends.len() && ends[end_pos].0 == primary {
            let (_, min, max, signature) = ends[end_pos];
            tree.add_with_signature(min, max + 1, -1, signature);
            end_pos += 1;
        }
    }
}

fn rectangle_signature(index: usize) -> [u64; 2] {
    [
        mix64(index as u64 ^ 0x243f_6a88_85a3_08d3),
        mix64(index as u64 ^ 0x1319_8a2e_0370_7344),
    ]
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn rank_rects(bounds: &[Rect<Scalar>]) -> Vec<Rect<usize>> {
    let mut x = Vec::with_capacity(bounds.len() * 2);
    let mut y = Vec::with_capacity(bounds.len() * 2);
    for item in bounds {
        x.push(&item.x_min);
        x.push(&item.x_max);
        y.push(&item.y_min);
        y.push(&item.y_max);
    }
    sort_dedup_scalars(&mut x);
    sort_dedup_scalars(&mut y);

    bounds
        .iter()
        .map(|item| Rect {
            x_min: rank_scalar(&x, &item.x_min),
            x_max: rank_scalar(&x, &item.x_max),
            y_min: rank_scalar(&y, &item.y_min),
            y_max: rank_scalar(&y, &item.y_max),
        })
        .collect()
}

fn sort_dedup_scalars(values: &mut Vec<&Scalar>) {
    values.sort_unstable_by(|left, right| scalar_cmp(left, right));
    values.dedup_by(|left, right| scalar_cmp(left, right) == Ordering::Equal);
}

fn scalar_cmp(left: &Scalar, right: &Scalar) -> Ordering {
    left.as_ref().cmp(&right.as_ref())
}

fn rank_scalar(sorted: &[&Scalar], value: &Scalar) -> usize {
    sorted
        .binary_search_by(|probe| scalar_cmp(probe, value))
        .expect("coordinate was collected before ranking")
}

struct RangeAddMaxTree {
    max: Vec<i32>,
    lazy: Vec<i32>,
    signature: Vec<[u64; 2]>,
    leaf_count: usize,
}

impl RangeAddMaxTree {
    fn new(len: usize) -> Self {
        let leaf_count = len.max(1).next_power_of_two();
        Self {
            max: vec![0; leaf_count * 2],
            lazy: vec![0; leaf_count * 2],
            signature: vec![[0; 2]; leaf_count * 2],
            leaf_count,
        }
    }

    fn add(&mut self, left: usize, right: usize, delta: i32) {
        self.add_with_signature(left, right, delta, [0; 2]);
    }

    fn add_with_signature(&mut self, left: usize, right: usize, delta: i32, signature: [u64; 2]) {
        self.add_inner(left, right, delta, signature, 1, 0..self.leaf_count);
    }

    fn max(&self) -> usize {
        self.max[1].max(0) as usize
    }

    fn argmax(&self) -> (usize, [u64; 2]) {
        let mut node = 1;
        let mut range = 0..self.leaf_count;
        let mut signature = [0; 2];
        loop {
            signature[0] ^= self.signature[node][0];
            signature[1] ^= self.signature[node][1];
            if node >= self.leaf_count {
                return (range.start, signature);
            }
            let mid = range.start + range.len() / 2;
            if self.max[node * 2] >= self.max[node * 2 + 1] {
                node *= 2;
                range.end = mid;
            } else {
                node = node * 2 + 1;
                range.start = mid;
            }
        }
    }

    fn add_inner(
        &mut self,
        left: usize,
        right: usize,
        delta: i32,
        signature: [u64; 2],
        node: usize,
        node_range: Range<usize>,
    ) {
        if right <= node_range.start || node_range.end <= left {
            return;
        }
        if left <= node_range.start && node_range.end <= right {
            self.max[node] += delta;
            self.lazy[node] += delta;
            self.signature[node][0] ^= signature[0];
            self.signature[node][1] ^= signature[1];
            return;
        }
        let mid = node_range.start + node_range.len() / 2;
        self.add_inner(
            left,
            right,
            delta,
            signature,
            node * 2,
            node_range.start..mid,
        );
        self.add_inner(
            left,
            right,
            delta,
            signature,
            node * 2 + 1,
            mid..node_range.end,
        );
        self.max[node] = self.lazy[node] + self.max[node * 2].max(self.max[node * 2 + 1]);
    }
}

/// Active y intervals used to count rectangle intersections during the x sweep.
/// Each Fenwick node stores counts for interval minima and maxima in one allocation.
struct ActiveYIntervals {
    endpoints: Vec<[u32; 2]>,
}

impl ActiveYIntervals {
    const MIN: usize = 0;
    const MAX: usize = 1;

    fn new(len: usize) -> Self {
        Self {
            endpoints: vec![[0; 2]; len + 1],
        }
    }

    fn add(&mut self, y_min: usize, y_max: usize, delta: i32) {
        self.add_endpoint(y_min, Self::MIN, delta);
        self.add_endpoint(y_max, Self::MAX, delta);
    }

    fn overlap_count(&self, y_min: usize, y_max: usize) -> usize {
        let below = self.prefix_sum(y_min, Self::MAX);
        let not_above = self.prefix_sum(y_max + 1, Self::MIN);
        not_above - below
    }

    fn add_endpoint(&mut self, index: usize, endpoint: usize, delta: i32) {
        let mut pos = index + 1;
        while pos < self.endpoints.len() {
            if delta >= 0 {
                self.endpoints[pos][endpoint] += delta as u32;
            } else {
                self.endpoints[pos][endpoint] -= delta.unsigned_abs();
            }
            pos += pos & pos.wrapping_neg();
        }
    }

    /// Number of endpoints whose compressed coordinate is strictly below `end`.
    fn prefix_sum(&self, end: usize, endpoint: usize) -> usize {
        let mut pos = end;
        let mut sum = 0u32;
        while pos > 0 {
            sum += self.endpoints[pos][endpoint];
            pos &= pos - 1;
        }
        sum as usize
    }
}

/// O(n log n) full-table Hilbert diagnostics. Scalar prefixes are intentionally ignored: the
/// information path reports the spatial quality of the two Hilbert dimensions and never builds a
/// potentially quadratic overlap graph. Each input is [x_min, x_max, y_min, y_max].
pub(crate) fn hilbert_diagnostics(bounds: &[[Scalar; 4]]) -> Result<(usize, u64)> {
    // Each dimension contributes two endpoints; every compressed rank must fit in u32.
    let max_blocks = (u32::MAX as usize).div_ceil(2);
    if bounds.len() > max_blocks {
        return Err(ErrorCode::Internal(format!(
            "Hilbert diagnostics supports at most {max_blocks} blocks"
        )));
    }
    if bounds.is_empty() {
        return Ok((0, 0));
    }

    let mut boxes = vec![Rect::default(); bounds.len()];
    let mut coordinates = Vec::with_capacity(bounds.len().saturating_mul(2));
    for item in bounds {
        coordinates.push(&item[0]);
        coordinates.push(&item[1]);
    }
    sort_dedup_scalars(&mut coordinates);
    for (item, ranked) in bounds.iter().zip(&mut boxes) {
        ranked.x_min = rank_scalar(&coordinates, &item[0]) as u32;
        ranked.x_max = rank_scalar(&coordinates, &item[1]) as u32;
    }

    coordinates.clear();
    for item in bounds {
        coordinates.push(&item[2]);
        coordinates.push(&item[3]);
    }
    sort_dedup_scalars(&mut coordinates);
    for (item, ranked) in bounds.iter().zip(&mut boxes) {
        ranked.y_min = rank_scalar(&coordinates, &item[2]) as u32;
        ranked.y_max = rank_scalar(&coordinates, &item[3]) as u32;
    }

    let x_count = boxes.iter().map(|item| item.x_max).max().unwrap_or(0) as usize + 1;
    let y_count = boxes.iter().map(|item| item.y_max).max().unwrap_or(0) as usize + 1;
    let mut starts = (0..boxes.len() as u32).collect::<Vec<_>>();
    let mut ends = starts.clone();
    starts.sort_unstable_by_key(|idx| (boxes[*idx as usize].x_min, *idx));
    ends.sort_unstable_by_key(|idx| (boxes[*idx as usize].x_max, *idx));

    let mut depth_tree = RangeAddMaxTree::new(y_count);
    let mut active_y = ActiveYIntervals::new(y_count);
    let mut overlap_pairs = 0u64;
    let mut max_depth = 0usize;
    let mut start_pos = 0usize;
    let mut end_pos = 0usize;

    for x in 0..x_count {
        while start_pos < starts.len() && boxes[starts[start_pos] as usize].x_min as usize == x {
            let item = boxes[starts[start_pos] as usize];
            let y_min = item.y_min as usize;
            let y_max = item.y_max as usize;
            overlap_pairs =
                overlap_pairs.saturating_add(active_y.overlap_count(y_min, y_max) as u64);
            depth_tree.add(y_min, y_max + 1, 1);
            active_y.add(y_min, y_max, 1);
            start_pos += 1;
        }
        max_depth = max_depth.max(depth_tree.max());

        while end_pos < ends.len() && boxes[ends[end_pos] as usize].x_max as usize == x {
            let item = boxes[ends[end_pos] as usize];
            let y_min = item.y_min as usize;
            let y_max = item.y_max as usize;
            depth_tree.add(y_min, y_max + 1, -1);
            active_y.add(y_min, y_max, -1);
            end_pos += 1;
        }
    }

    Ok((max_depth, overlap_pairs))
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn stats(rect: (i32, i32, i32, i32)) -> ClusterStatistics {
        ClusterStatistics::new(
            0,
            vec![scalar(rect.0), scalar(rect.2)],
            vec![scalar(rect.1), scalar(rect.3)],
            0,
        )
    }

    fn bounds(stats: &[ClusterStatistics]) -> Vec<Rect<Scalar>> {
        stats
            .iter()
            .map(Rect::try_from_stats)
            .collect::<Result<_>>()
            .unwrap()
    }

    #[test]
    fn test_hilbert_depth_and_diagnostics() {
        let overlapping = [stats((0, 2, 0, 2)), stats((1, 3, 1, 3))];

        let bounds = overlapping.map(|stats| {
            let rect = Rect::try_from_stats(&stats).unwrap();
            [rect.x_min, rect.x_max, rect.y_min, rect.y_max]
        });
        assert_eq!(hilbert_diagnostics(&bounds).unwrap(), (2, 1));

        let touching = [scalar(0), scalar(1), scalar(0), scalar(1)];
        let adjacent = [scalar(1), scalar(2), scalar(1), scalar(2)];
        assert_eq!(hilbert_diagnostics(&[touching, adjacent]).unwrap(), (2, 1));
    }

    #[test]
    fn test_top_witnesses_respect_mbrs_and_budget() {
        let block_stats = [
            stats((0, 5, 0, 5)),
            stats((1, 4, 1, 4)),
            stats((2, 3, 2, 3)),
            stats((20, 25, 20, 25)),
            stats((21, 24, 21, 24)),
        ];
        let boxes = rank_rects(&bounds(&block_stats));
        let witnesses = top_witnesses(&boxes, 8);
        let candidates = witnesses
            .iter()
            .copied()
            .map(|witness| members_at_witness(&boxes, witness))
            .collect::<Vec<_>>();
        assert_eq!(candidates[0], vec![0, 1, 2]);
        assert!(candidates.contains(&vec![3, 4]));
        assert_eq!(top_witnesses(&boxes, 1).len(), 1);
    }

    #[test]
    fn test_sweep_matches_brute_force() {
        let mut seed = 0x9e37_79b9_u32;
        for rectangle_count in 1..=12 {
            for _ in 0..64 {
                let mut boxes = Vec::with_capacity(rectangle_count);
                for _ in 0..rectangle_count {
                    let mut next = || {
                        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                        (seed % 9) as usize
                    };
                    let (x1, x2) = (next(), next());
                    let (y1, y2) = (next(), next());
                    boxes.push(Rect {
                        x_min: x1.min(x2),
                        x_max: x1.max(x2),
                        y_min: y1.min(y2),
                        y_max: y1.max(y2),
                    });
                }

                let brute_max = (0..=8)
                    .flat_map(|x| (0..=8).map(move |y| (x, y)))
                    .map(|(x, y)| {
                        boxes
                            .iter()
                            .filter(|item| {
                                item.x_min <= x
                                    && x <= item.x_max
                                    && item.y_min <= y
                                    && y <= item.y_max
                            })
                            .count()
                    })
                    .max()
                    .unwrap_or(0);
                let mut sweep_max = usize::from(!boxes.is_empty());
                sweep_xy(&boxes, false, |witness| {
                    sweep_max = sweep_max.max(witness.depth);
                });
                assert_eq!(sweep_max, brute_max);

                for witness in top_witnesses(&boxes, boxes.len() * 4) {
                    let members = members_at_witness(&boxes, witness);
                    assert_eq!(witness.depth, members.len());
                    let signature = members.iter().fold([0; 2], |mut signature, idx| {
                        let item = rectangle_signature(*idx);
                        signature[0] ^= item[0];
                        signature[1] ^= item[1];
                        signature
                    });
                    assert_eq!(witness.signature, signature);
                }
            }
        }
    }
}
