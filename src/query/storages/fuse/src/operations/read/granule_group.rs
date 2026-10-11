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

use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub(crate) fn build_granule_groups(
    ranges: Option<&[Range<usize>]>,
    granule_rows: usize,
    block_rows: usize,
    max_block_rows: usize,
) -> Result<Vec<Vec<Range<usize>>>> {
    if granule_rows == 0 {
        return Err(ErrorCode::Internal(
            "granule group builder cannot use zero granule rows",
        ));
    }

    let num_granules = block_rows.div_ceil(granule_rows);
    if num_granules == 0 {
        return Ok(Vec::new());
    }

    let full_range;
    let ranges = match ranges {
        Some([]) => {
            return Err(ErrorCode::Internal(
                "granule group builder cannot use empty ranges",
            ));
        }
        Some(ranges) => ranges,
        None => {
            full_range = 0..num_granules;
            std::slice::from_ref(&full_range)
        }
    };

    let mut normalized: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end || range.end > num_granules {
            return Err(ErrorCode::Internal(format!(
                "invalid granule range {range:?} for {num_granules} granules"
            )));
        }
        if let Some(last) = normalized.last_mut() {
            if range.start < last.end {
                return Err(ErrorCode::Internal(format!(
                    "overlapping or unordered granule ranges {last:?} and {range:?}"
                )));
            }
            if range.start == last.end {
                last.end = range.end;
                continue;
            }
        }
        normalized.push(range.clone());
    }

    let max_block_rows = max_block_rows.max(1);
    let mut chunks = Vec::new();
    for range in normalized {
        let mut start = range.start;
        while start < range.end {
            let mut end = start;
            let mut rows = 0;
            while end < range.end {
                let next_range = end..end + 1;
                let next_rows = granule_row_count(&next_range, granule_rows, block_rows)?;
                if end > start && rows + next_rows > max_block_rows {
                    break;
                }
                rows += next_rows;
                end += 1;
                if rows >= max_block_rows {
                    break;
                }
            }
            chunks.push((start..end, rows));
            start = end;
        }
    }

    let mut groups = Vec::new();
    let mut current_ranges = Vec::new();
    let mut current_rows = 0;
    for (range, rows) in chunks {
        if !current_ranges.is_empty() && current_rows + rows > max_block_rows {
            groups.push(std::mem::take(&mut current_ranges));
            current_rows = 0;
        }
        current_ranges.push(range);
        current_rows += rows;
        if current_rows >= max_block_rows {
            groups.push(std::mem::take(&mut current_ranges));
            current_rows = 0;
        }
    }
    if !current_ranges.is_empty() {
        groups.push(current_ranges);
    }
    Ok(groups)
}

fn granule_row_count(
    range: &Range<usize>,
    granule_rows: usize,
    block_rows: usize,
) -> Result<usize> {
    let start = range
        .start
        .checked_mul(granule_rows)
        .ok_or_else(|| ErrorCode::Internal("granule range row offset overflows"))?
        .min(block_rows);
    let end = range
        .end
        .checked_mul(granule_rows)
        .ok_or_else(|| ErrorCode::Internal("granule range row offset overflows"))?
        .min(block_rows);
    Ok(end.saturating_sub(start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_granule_groups_merges_splits_and_packs() {
        let ranges = [0..2, 2..4, 6..7, 9..14];
        let groups = build_granule_groups(Some(&ranges), 100, 1350, 400).unwrap();
        assert_eq!(groups, vec![vec![0..4], vec![6..7], vec![9..13], vec![
            13..14
        ]]);
    }

    #[test]
    fn test_build_granule_groups_packs_disjoint_ranges() {
        let ranges = [0..2, 4..5, 8..10];
        let groups = build_granule_groups(Some(&ranges), 100, 1000, 400).unwrap();
        assert_eq!(groups, vec![vec![0..2, 4..5], vec![8..10]]);
    }

    #[test]
    fn test_build_granule_groups_uses_full_range() {
        let groups = build_granule_groups(None, 100, 950, 250).unwrap();
        assert_eq!(groups, vec![
            vec![0..2],
            vec![2..4],
            vec![4..6],
            vec![6..8],
            vec![8..10]
        ]);
    }

    #[test]
    fn test_build_granule_groups_counts_partial_tail() {
        let groups = build_granule_groups(None, 100, 250, 220).unwrap();
        assert_eq!(groups, vec![vec![0..2], vec![2..3]]);
    }

    #[test]
    fn test_build_granule_groups_allows_one_oversized_granule() {
        let range = 1..3;
        let groups =
            build_granule_groups(Some(std::slice::from_ref(&range)), 100, 250, 64).unwrap();
        assert_eq!(groups, vec![vec![1..2], vec![2..3]]);
    }

    #[test]
    fn test_build_granule_groups_handles_empty_block() {
        assert!(build_granule_groups(None, 100, 0, 400).unwrap().is_empty());
    }

    #[test]
    fn test_build_granule_groups_rejects_invalid_ranges() {
        assert!(build_granule_groups(Some(&[]), 100, 1000, 400).is_err());
        let empty_range = 2..2;
        assert!(
            build_granule_groups(Some(std::slice::from_ref(&empty_range)), 100, 1000, 400).is_err()
        );
        let overlapping = [2..4, 3..5];
        assert!(build_granule_groups(Some(&overlapping), 100, 1000, 400).is_err());
        let out_of_bounds = 9..11;
        assert!(
            build_granule_groups(Some(std::slice::from_ref(&out_of_bounds)), 100, 1000, 400,)
                .is_err()
        );
        assert!(build_granule_groups(None, 0, 1000, 400).is_err());
    }
}
