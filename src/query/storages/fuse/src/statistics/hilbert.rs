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
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;

#[derive(Clone, Copy, Default)]
struct Rect<T> {
    x_min: T,
    x_max: T,
    y_min: T,
    y_max: T,
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
    leaf_count: usize,
}

impl RangeAddMaxTree {
    fn new(len: usize) -> Self {
        let leaf_count = len.max(1).next_power_of_two();
        Self {
            max: vec![0; leaf_count * 2],
            lazy: vec![0; leaf_count * 2],
            leaf_count,
        }
    }

    fn add(&mut self, left: usize, right: usize, delta: i32) {
        self.add_inner(left, right, delta, 1, 0..self.leaf_count);
    }

    fn max(&self) -> usize {
        self.max[1].max(0) as usize
    }

    fn add_inner(
        &mut self,
        left: usize,
        right: usize,
        delta: i32,
        node: usize,
        node_range: Range<usize>,
    ) {
        if right <= node_range.start || node_range.end <= left {
            return;
        }
        if left <= node_range.start && node_range.end <= right {
            self.max[node] += delta;
            self.lazy[node] += delta;
            return;
        }
        let mid = node_range.start + node_range.len() / 2;
        self.add_inner(left, right, delta, node * 2, node_range.start..mid);
        self.add_inner(left, right, delta, node * 2 + 1, mid..node_range.end);
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

    #[test]
    fn test_hilbert_depth_and_diagnostics() {
        let overlapping = [[scalar(0), scalar(2), scalar(0), scalar(2)], [
            scalar(1),
            scalar(3),
            scalar(1),
            scalar(3),
        ]];
        assert_eq!(hilbert_diagnostics(&overlapping).unwrap(), (2, 1));

        let touching = [scalar(0), scalar(1), scalar(0), scalar(1)];
        let adjacent = [scalar(1), scalar(2), scalar(1), scalar(2)];
        assert_eq!(hilbert_diagnostics(&[touching, adjacent]).unwrap(), (2, 1));
    }
}
