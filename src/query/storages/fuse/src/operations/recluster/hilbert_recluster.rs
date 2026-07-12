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

use std::borrow::Borrow;
use std::cmp;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use indexmap::IndexSet;
use rstar::AABB;
use rstar::RTree;
use rstar::RTreeObject;

use super::overlap_selection::OverlapSegmentItem;
use super::overlap_selection::OverlapSelector;
use super::overlap_selection::collect_overlap_window_segments;
use super::overlap_selection::identity_overlaps;
use super::overlap_selection::refine_scalar_windows_by_overlap;
use super::overlap_selection::select_overlap_only_segments;
use super::recluster_mutator::ReclusterBlock;
use super::recluster_mutator::ReclusterGroup;
use super::recluster_mutator::ReclusterMutator;
use super::recluster_mutator::ReclusterTaskCandidate;
use super::recluster_mutator::SelectedReclusterSegment;
use crate::SegmentLocation;
use crate::statistics::BlockOverlapDepth;
use crate::statistics::cluster_stats_hilbert_minmax;
use crate::statistics::cluster_stats_scalar_overlap;

// Hilbert marker accepts at most five dimensions. RTree envelopes use a fixed
// array so all candidate boxes have one concrete type.
const HILBERT_RTREE_DIMS: usize = 5;
type HilbertRTreeEnvelope = AABB<[i64; HILBERT_RTREE_DIMS]>;

/// Hilbert overlap graph plus normalized boxes used to score hotspots.
pub(crate) struct HilbertCandidates {
    // Adjacency graph of boxes whose Hilbert dimensions overlap.
    overlaps: Vec<IndexSet<usize>>,
    // Normalized bounding boxes used by depth scoring.
    boxes: Vec<HilbertBox>,
    // Active dimension count; the fixed arrays may have unused trailing slots.
    dims: usize,
}

impl HilbertCandidates {
    pub(crate) fn max_depth(&self) -> usize {
        (0..self.overlaps.len())
            .map(|idx| self.overlap_depth(idx))
            .max()
            .unwrap_or(0)
    }

    /// Convert the overlap graph into per-box clustering information stats.
    pub(crate) fn overlap_depths(&self) -> Vec<BlockOverlapDepth> {
        (0..self.overlaps.len())
            .map(|idx| {
                let overlap = self.overlaps[idx].len().saturating_sub(1);
                let depth = self.overlap_depth(idx);
                BlockOverlapDepth { overlap, depth }
            })
            .collect()
    }

    fn overlap_depth(&self, idx: usize) -> usize {
        self.members_depth_score(
            idx,
            self.overlaps[idx].iter().copied(),
            self.overlaps[idx].len(),
        )
    }

    fn remaining_depth_score(&self, seed: usize, used: &[bool]) -> usize {
        if used[seed] {
            return 0;
        }

        self.members_depth_score(
            seed,
            self.overlaps[seed]
                .iter()
                .copied()
                .filter(|idx| !used[*idx]),
            0,
        )
    }

    fn members_depth_score<I>(&self, seed: usize, members: I, initial_capacity: usize) -> usize
    where I: IntoIterator<Item = usize> {
        if self.dims == 0 {
            return members.into_iter().count();
        }

        let mut members = members.into_iter();
        let Some(first) = members.next() else {
            return 0;
        };
        let Some(second) = members.next() else {
            return 1;
        };
        let Some(third) = members.next() else {
            return 2;
        };

        let seed_box = &self.boxes[seed];
        let mut clipped_boxes = Vec::with_capacity(initial_capacity);
        // Score the hotspot around the seed, not the whole overlap component:
        // each member is clipped to the seed box before computing stabbing depth.
        for idx in [first, second, third].into_iter().chain(members) {
            let item_box = &self.boxes[idx];
            let mut lower = [0_i64; HILBERT_RTREE_DIMS];
            let mut upper = [0_i64; HILBERT_RTREE_DIMS];
            let mut overlaps_seed = true;
            for dim in 0..self.dims {
                lower[dim] = item_box.lower[dim].max(seed_box.lower[dim]);
                upper[dim] = item_box.upper[dim].min(seed_box.upper[dim]);
                if lower[dim] > upper[dim] {
                    overlaps_seed = false;
                    break;
                }
            }
            if overlaps_seed {
                clipped_boxes.push(HilbertBox { lower, upper });
            }
        }

        if clipped_boxes.is_empty() {
            return 0;
        }
        if self.dims == 2 {
            return hilbert_max_stabbing_depth_2d(&clipped_boxes);
        }

        let candidates = (0..clipped_boxes.len()).collect::<Vec<_>>();
        hilbert_max_stabbing_depth_at_dim(&clipped_boxes, self.dims, 0, &candidates, 0)
    }
}

struct HilbertOverlapSelector {
    candidates: HilbertCandidates,
    blocked_seeds: Vec<bool>,
}

impl HilbertOverlapSelector {
    fn new(candidates: HilbertCandidates) -> Self {
        let blocked_seeds = vec![false; candidates.overlaps.len()];
        Self {
            candidates,
            blocked_seeds,
        }
    }
}

struct HilbertReclusterSegment {
    segment: SelectedReclusterSegment,
    stats: ClusterStatistics,
}

impl OverlapSegmentItem for HilbertReclusterSegment {
    fn from_selected_segment(
        segment: SelectedReclusterSegment,
        stats: ClusterStatistics,
    ) -> Result<Self> {
        Ok(Self { segment, stats })
    }

    fn selected_segment(&self) -> &SelectedReclusterSegment {
        &self.segment
    }
}

impl Borrow<ClusterStatistics> for HilbertReclusterSegment {
    fn borrow(&self) -> &ClusterStatistics {
        &self.stats
    }
}

impl OverlapSelector for HilbertOverlapSelector {
    const LABEL: &'static str = "hilbert";

    fn depth_summary(&self) -> (usize, usize) {
        let mut max_depth = 0usize;
        let mut sum_depth = 0usize;
        for idx in 0..self.candidates.overlaps.len() {
            let depth = self.candidates.overlap_depth(idx);
            max_depth = max_depth.max(depth);
            sum_depth += depth;
        }
        (max_depth, sum_depth)
    }

    fn next_seed(&mut self, used: &[bool]) -> Option<(usize, usize)> {
        let mut best = None;
        for idx in 0..self.candidates.overlaps.len() {
            if used[idx] || self.blocked_seeds[idx] {
                continue;
            }

            let depth = self.candidates.remaining_depth_score(idx, used);
            if depth <= 1 {
                continue;
            }
            if best.is_none_or(|(best_idx, best_depth)| {
                depth > best_depth || (depth == best_depth && idx < best_idx)
            }) {
                best = Some((idx, depth));
            }
        }
        best
    }

    fn members_by_priority(&self, seed: usize, used: &[bool]) -> Vec<usize> {
        let mut members = self.candidates.overlaps[seed]
            .iter()
            .copied()
            .filter(|idx| !used[*idx])
            .map(|idx| (idx, self.candidates.remaining_depth_score(idx, used)))
            .collect::<Vec<_>>();

        members.sort_by(|(left_idx, left_score), (right_idx, right_score)| {
            right_score
                .cmp(left_score)
                .then_with(|| {
                    self.candidates.overlaps[*right_idx]
                        .len()
                        .cmp(&self.candidates.overlaps[*left_idx].len())
                })
                .then_with(|| left_idx.cmp(right_idx))
        });
        members.into_iter().map(|(idx, _)| idx).collect()
    }

    fn block_seed(&mut self, seed: usize) {
        self.blocked_seeds[seed] = true;
    }
}

pub(super) fn fetch_hilbert_task_candidates(
    mutator: &ReclusterMutator,
    group: ReclusterGroup,
    indices: &[usize],
    blocks: &[&ReclusterBlock],
    task_budget: usize,
) -> Result<Vec<ReclusterTaskCandidate>> {
    let cluster_stats = indices
        .iter()
        .map(|idx| blocks[*idx].stats())
        .collect::<Vec<_>>();
    let require_scalar_overlap = !mutator.cluster_key_types.is_empty();
    let hilbert_candidates = build_hilbert_candidates(&cluster_stats, |left, right| {
        !require_scalar_overlap
            || cluster_stats_scalar_overlap(
                blocks[indices[left]].stats(),
                blocks[indices[right]].stats(),
            )
    });
    mutator.fetch_overlap_task_candidates(
        group,
        indices,
        blocks,
        HilbertOverlapSelector::new(hilbert_candidates),
        task_budget,
    )
}

pub(super) fn select_hilbert_segments(
    mutator: &ReclusterMutator,
    compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
    window_len: usize,
) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
    if mutator.cluster_key_types.is_empty() {
        select_overlap_only_segments::<HilbertReclusterSegment, HilbertOverlapSelector, _>(
            mutator,
            compact_segments,
            window_len,
            |hilbert_segments| {
                let candidates = build_hilbert_candidates(hilbert_segments, |_, _| true);
                Ok(HilbertOverlapSelector::new(candidates))
            },
        )
    } else {
        let scalar_windows = mutator.select_scalar_segments(compact_segments, window_len)?;
        refine_scalar_windows_by_overlap(scalar_windows, window_len, |scalar_window| {
            let hilbert_segments =
                collect_overlap_window_segments::<HilbertReclusterSegment>(mutator, scalar_window)?;
            let HilbertCandidates { overlaps, .. } =
                build_hilbert_candidates(&hilbert_segments, |left, right| {
                    cluster_stats_scalar_overlap(
                        &hilbert_segments[left].stats,
                        &hilbert_segments[right].stats,
                    )
                });

            Ok((hilbert_segments, overlaps))
        })
    }
}

#[derive(Clone, Copy)]
struct HilbertBox {
    lower: [i64; HILBERT_RTREE_DIMS],
    upper: [i64; HILBERT_RTREE_DIMS],
}

impl HilbertBox {
    fn envelope(&self) -> HilbertRTreeEnvelope {
        HilbertRTreeEnvelope::from_corners(self.lower, self.upper)
    }
}

struct HilbertRTreeBox {
    bounds: HilbertBox,
    index: usize,
}

impl RTreeObject for HilbertRTreeBox {
    type Envelope = HilbertRTreeEnvelope;

    fn envelope(&self) -> Self::Envelope {
        self.bounds.envelope()
    }
}

/// Build Hilbert overlap candidates from persisted Hilbert dimension stats.
pub(crate) fn build_hilbert_candidates<S, F>(
    cluster_stats: &[S],
    pair_filter: F,
) -> HilbertCandidates
where
    S: Borrow<ClusterStatistics>,
    F: Fn(usize, usize) -> bool,
{
    let hilbert_minmax = cluster_stats
        .iter()
        .map(|stats| cluster_stats_hilbert_minmax(stats.borrow()))
        .collect::<Vec<_>>();
    let dims = hilbert_minmax
        .iter()
        .filter_map(|stats| stats.map(|(min, _)| min.len()))
        .max()
        .unwrap_or(0)
        .min(HILBERT_RTREE_DIMS);
    let mut dimension_values = (0..dims)
        .map(|_| Vec::with_capacity(cluster_stats.len() * 2))
        .collect::<Vec<_>>();
    for stats in &hilbert_minmax {
        let Some((min, max)) = *stats else {
            continue;
        };
        for dim in 0..dims.min(min.len()) {
            dimension_values[dim].push(&min[dim]);
            dimension_values[dim].push(&max[dim]);
        }
    }

    let dimension_values = dimension_values
        .into_iter()
        .map(|mut values| {
            let mut comparable = true;
            values.sort_by(|left, right| match (*left).partial_cmp(*right) {
                Some(ordering) => ordering,
                None => {
                    comparable = false;
                    cmp::Ordering::Equal
                }
            });
            if values.is_empty() || !comparable {
                return None;
            }

            values
                .dedup_by(|left, right| (*left).partial_cmp(*right) == Some(cmp::Ordering::Equal));
            Some(values)
        })
        .collect::<Vec<_>>();

    // Convert scalar min/max values into dense integer ranks. RTree needs a
    // numeric coordinate space, and ranks preserve ordering without depending on
    // the original scalar type or scale.
    let mut max_coords = [0_i64; HILBERT_RTREE_DIMS];
    for (dim, values) in dimension_values.iter().enumerate() {
        if let Some(values) = values {
            max_coords[dim] = values.len().saturating_sub(1) as i64;
        }
    }

    let boxes = hilbert_minmax
        .iter()
        .map(|stats| {
            let mut lower = [0_i64; HILBERT_RTREE_DIMS];
            let mut upper = max_coords;
            if let Some((min, max)) = *stats {
                for dim in 0..dims.min(min.len()) {
                    let Some(values) = &dimension_values[dim] else {
                        continue;
                    };
                    let rank = |value: &Scalar| {
                        values
                            .binary_search_by(|probe| {
                                (*probe).partial_cmp(value).unwrap_or(cmp::Ordering::Equal)
                            })
                            .ok()
                            .map(|idx| idx as i64)
                    };
                    let Some(min_rank) = rank(&min[dim]) else {
                        continue;
                    };
                    let Some(max_rank) = rank(&max[dim]) else {
                        continue;
                    };
                    lower[dim] = min_rank.min(max_rank);
                    upper[dim] = min_rank.max(max_rank);
                }
            }
            HilbertBox { lower, upper }
        })
        .collect::<Vec<_>>();
    let mut overlaps = identity_overlaps(cluster_stats.len());
    if cluster_stats.len() < 2 {
        return HilbertCandidates {
            overlaps,
            boxes,
            dims,
        };
    }

    let tree = RTree::bulk_load(
        boxes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, bounds)| HilbertRTreeBox { bounds, index })
            .collect(),
    );
    // RTree proposes bbox intersections; pair_filter applies scalar-prefix
    // constraints before the final Hilbert dimension stats overlap check.
    for (left, item) in boxes.iter().enumerate() {
        for candidate in tree.locate_in_envelope_intersecting(&item.envelope()) {
            let right = candidate.index;
            if right <= left {
                continue;
            }

            if !pair_filter(left, right) {
                continue;
            }

            let hilbert_overlap = match (hilbert_minmax[left], hilbert_minmax[right]) {
                (Some((left_min, left_max)), Some((right_min, right_max))) => {
                    (0..dims).all(|dim| {
                        if dim >= left_min.len() || dim >= right_min.len() {
                            return true;
                        }
                        left_min[dim].partial_cmp(&right_max[dim]) != Some(cmp::Ordering::Greater)
                            && right_min[dim].partial_cmp(&left_max[dim])
                                != Some(cmp::Ordering::Greater)
                    })
                }
                _ => true,
            };
            if hilbert_overlap {
                overlaps[left].insert(right);
                overlaps[right].insert(left);
            }
        }
    }

    HilbertCandidates {
        overlaps,
        boxes,
        dims,
    }
}

fn hilbert_max_stabbing_depth_2d(boxes: &[HilbertBox]) -> usize {
    // Common two-dimensional case: sweep x-events and keep the active y-depth
    // with a range-add max tree.
    let mut y_coords = Vec::with_capacity(boxes.len() * 2);
    for item in boxes {
        y_coords.push(item.lower[1]);
        y_coords.push(item.upper[1]);
    }
    y_coords.sort_unstable();
    y_coords.dedup();
    if y_coords.is_empty() {
        return 0;
    }

    let mut events = Vec::with_capacity(boxes.len() * 2);
    for item in boxes {
        let y_low = y_coords
            .binary_search(&item.lower[1])
            .expect("lower y coordinate collected before sorting");
        let y_high = y_coords
            .binary_search(&item.upper[1])
            .expect("upper y coordinate collected before sorting");
        events.push((item.lower[0], 1_i32, y_low, y_high));
        events.push((item.upper[0], -1_i32, y_low, y_high));
    }
    events.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));

    debug_assert!(!y_coords.is_empty());
    let leaf_count = y_coords.len().next_power_of_two();
    let mut tree = HilbertRangeAddMaxTree {
        max: vec![0; leaf_count * 2],
        lazy: vec![0; leaf_count * 2],
        leaf_count,
    };
    let mut max_depth = 0usize;
    for (_, delta, y_low, y_high) in events {
        tree.add_inner(y_low, y_high + 1, delta, 1, 0, tree.leaf_count);
        if delta > 0 {
            max_depth = max_depth.max(tree.max[1].max(0) as usize);
        }
    }
    max_depth
}

struct HilbertRangeAddMaxTree {
    max: Vec<i32>,
    lazy: Vec<i32>,
    leaf_count: usize,
}

impl HilbertRangeAddMaxTree {
    fn add_inner(
        &mut self,
        left: usize,
        right: usize,
        delta: i32,
        node: usize,
        node_left: usize,
        node_right: usize,
    ) {
        if right <= node_left || node_right <= left {
            return;
        }
        if left <= node_left && node_right <= right {
            self.max[node] += delta;
            self.lazy[node] += delta;
            return;
        }

        let mid = node_left + ((node_right - node_left) / 2);
        self.add_inner(left, right, delta, node * 2, node_left, mid);
        self.add_inner(left, right, delta, node * 2 + 1, mid, node_right);
        self.max[node] = self.lazy[node] + self.max[node * 2].max(self.max[node * 2 + 1]);
    }
}

fn hilbert_max_stabbing_depth_at_dim(
    boxes: &[HilbertBox],
    dims: usize,
    dim: usize,
    candidates: &[usize],
    mut best: usize,
) -> usize {
    if candidates.len() <= best {
        return best;
    }
    let mut common_intersection = true;
    'dim_loop: for dim in 0..dims {
        let mut lower = i64::MIN;
        let mut upper = i64::MAX;
        for idx in candidates {
            lower = lower.max(boxes[*idx].lower[dim]);
            upper = upper.min(boxes[*idx].upper[dim]);
            if lower > upper {
                common_intersection = false;
                break 'dim_loop;
            }
        }
    }
    if common_intersection {
        return candidates.len();
    }
    if dim >= dims {
        return best.max(candidates.len());
    }
    if dim + 1 == dims {
        // Last dimension reduces to a one-dimensional sweep.
        let mut events = Vec::with_capacity(candidates.len() * 2);
        for idx in candidates {
            events.push((boxes[*idx].lower[dim], 1_i32));
            events.push((boxes[*idx].upper[dim], -1_i32));
        }
        events.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));

        let mut depth = 0usize;
        let mut max_depth = 0usize;
        for (_, delta) in events {
            if delta > 0 {
                depth += delta as usize;
                max_depth = max_depth.max(depth);
            } else {
                depth -= (-delta) as usize;
            }
        }
        return best.max(max_depth);
    }

    let mut coords = candidates
        .iter()
        .map(|idx| boxes[*idx].lower[dim])
        .collect::<Vec<_>>();
    coords.sort_unstable();
    coords.dedup();

    // Branch only on lower-bound coordinates. Any maximum stabbing point can be
    // shifted to a lower bound without reducing the active box count.
    let mut branches = Vec::with_capacity(coords.len());
    for coord in coords {
        let next = candidates
            .iter()
            .copied()
            .filter(|idx| boxes[*idx].lower[dim] <= coord && coord <= boxes[*idx].upper[dim])
            .collect::<Vec<_>>();
        if next.len() > best {
            branches.push(next);
        }
    }
    branches.sort_by_key(|branch| cmp::Reverse(branch.len()));

    for next in branches {
        if next.len() <= best {
            continue;
        }
        best = hilbert_max_stabbing_depth_at_dim(boxes, dims, dim + 1, &next, best);
        if best == candidates.len() {
            break;
        }
    }

    best
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn int32_scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn stats(ranges: &[(i32, i32)]) -> ClusterStatistics {
        ClusterStatistics::new(
            0,
            vec![Scalar::Tuple(
                ranges.iter().map(|(min, _)| int32_scalar(*min)).collect(),
            )],
            vec![Scalar::Tuple(
                ranges.iter().map(|(_, max)| int32_scalar(*max)).collect(),
            )],
            0,
            None,
        )
    }

    #[test]
    fn test_hilbert_rtree_overlaps_match_bbox_intersection() {
        let blocks = [
            stats(&[(0, 1), (0, 1)]),
            stats(&[(3, 4), (3, 4)]),
            stats(&[(1, 3), (1, 3)]),
        ];

        let candidates = build_hilbert_candidates(&blocks, |_, _| true);
        let overlaps = &candidates.overlaps;

        assert!(overlaps[0].contains(&0));
        assert!(overlaps[0].contains(&2));
        assert!(!overlaps[0].contains(&1));

        assert!(overlaps[1].contains(&2));
        assert!(overlaps[2].contains(&1));
    }

    #[test]
    fn test_hilbert_rtree_pair_filter_is_applied_before_scoring() {
        let blocks = [
            stats(&[(0, 2), (0, 2)]),
            stats(&[(1, 3), (1, 3)]),
            stats(&[(1, 3), (1, 3)]),
        ];

        let candidates =
            build_hilbert_candidates(&blocks, |left, right| !(left == 0 && right == 2));
        let overlaps = &candidates.overlaps;

        assert!(overlaps[0].contains(&1));
        assert!(!overlaps[0].contains(&2));
        assert!(overlaps[1].contains(&2));
    }

    #[test]
    fn test_hilbert_overlap_depth_uses_generic_five_dimensional_score() {
        let blocks = [
            stats(&[(0, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(0, 4), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(6, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(0, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
        ];

        let candidates = build_hilbert_candidates(&blocks, |_, _| true);
        let overlaps = &candidates.overlaps;
        let scores = (0..candidates.overlaps.len())
            .map(|idx| candidates.overlap_depth(idx))
            .collect::<Vec<_>>();

        assert_eq!(overlaps[0].len(), 4);
        assert_eq!(scores[0], 3);
        assert_eq!(scores[1], 3);
        assert_eq!(scores[2], 3);
    }

    #[test]
    fn test_hilbert_overlap_depth_combines_lower_bounds_across_dimensions() {
        let blocks = [
            stats(&[(0, 10), (0, 10), (0, 10)]),
            stats(&[(5, 6), (0, 10), (0, 10)]),
            stats(&[(0, 10), (5, 6), (0, 10)]),
        ];

        let candidates = build_hilbert_candidates(&blocks, |_, _| true);
        let overlaps = &candidates.overlaps;
        let scores = (0..candidates.overlaps.len())
            .map(|idx| candidates.overlap_depth(idx))
            .collect::<Vec<_>>();

        assert_eq!(overlaps[0].len(), 3);
        assert_eq!(scores[0], 3);
    }

    #[test]
    fn test_hilbert_overlap_depth_2d_sweep_line_matches_generic_depth() {
        let boxes = vec![
            HilbertBox {
                lower: [0, 0, 0, 0, 0],
                upper: [10, 10, 0, 0, 0],
            },
            HilbertBox {
                lower: [2, 2, 0, 0, 0],
                upper: [5, 9, 0, 0, 0],
            },
            HilbertBox {
                lower: [4, 1, 0, 0, 0],
                upper: [8, 6, 0, 0, 0],
            },
            HilbertBox {
                lower: [7, 7, 0, 0, 0],
                upper: [9, 9, 0, 0, 0],
            },
        ];
        let candidates = (0..boxes.len()).collect::<Vec<_>>();

        assert_eq!(hilbert_max_stabbing_depth_2d(&boxes), 3);
        assert_eq!(
            hilbert_max_stabbing_depth_2d(&boxes),
            hilbert_max_stabbing_depth_at_dim(&boxes, 2, 0, &candidates, 0)
        );
    }

    #[test]
    fn test_hilbert_remaining_depth_score_recomputes_after_peeling() {
        let blocks = [
            stats(&[(0, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(0, 4), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(6, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
            stats(&[(0, 10), (0, 10), (0, 10), (0, 10), (0, 10)]),
        ];
        let candidates = build_hilbert_candidates(&blocks, |_, _| true);

        let used = vec![false; blocks.len()];
        assert_eq!(candidates.remaining_depth_score(0, &used), 3);

        let mut used = vec![false; blocks.len()];
        used[3] = true;
        assert_eq!(candidates.remaining_depth_score(0, &used), 2);
    }
}
