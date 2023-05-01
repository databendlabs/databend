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

/// Merge the overlap range.
/// If we have 3 ranges: [1,3], [2,4], [6,8]
/// The final range stack will be: [1,4], [6,8]
/// If we set the max_merge_gap to 2:
/// The final range stack will be: [1,8]
use std::cmp;
use std::ops::Range;

/// Check overlap.
fn overlaps(this: &Range<u64>, other: &Range<u64>, max_gap_size: u64, max_range_size: u64) -> bool {
    let this_len = this.end - this.start;
    if this_len >= max_range_size {
        false
    } else {
        let end_with_gap = this.end + max_gap_size;
        (other.start >= this.start && other.start <= end_with_gap)
            || (other.end >= this.start && other.end <= end_with_gap)
    }
}

/// Merge to one.
fn merge(this: &mut Range<u64>, other: &Range<u64>) {
    this.start = cmp::min(this.start, other.start);
    this.end = cmp::max(this.end, other.end);
}

#[derive(Debug, Clone)]
pub struct RangeMerger {
    max_gap_size: u64,
    max_range_size: u64,
    ranges: Vec<Range<u64>>,
}

impl RangeMerger {
    pub fn from_iter<I>(iter: I, max_gap_size: u64, max_range_size: u64) -> Self
    where I: IntoIterator<Item = Range<u64>> {
        let mut raw_ranges: Vec<_> = iter.into_iter().collect();
        raw_ranges.sort_by(|a, b| a.start.cmp(&b.start));

        let mut rs = RangeMerger {
            max_gap_size,
            max_range_size,
            ranges: Vec::with_capacity(raw_ranges.len()),
        };

        for range in &raw_ranges {
            rs.add(range);
        }

        rs
    }

    // Get the merged range with a range.
    // Merged range must larger than check range.
    pub fn get(&self, check: Range<u64>) -> Option<(usize, Range<u64>)> {
        for (i, r) in self.ranges.iter().enumerate() {
            if r.start <= check.start && r.end >= check.end {
                return Some((i, r.clone()));
            }
        }
        None
    }

    pub fn ranges(&self) -> Vec<Range<u64>> {
        self.ranges.clone()
    }

    fn add(&mut self, range: &Range<u64>) {
        if let Some(last) = self.ranges.last_mut() {
            if overlaps(last, range, self.max_gap_size, self.max_range_size) {
                merge(last, range);
                return;
            }
        }

        self.ranges.push(range.clone());
    }
}
