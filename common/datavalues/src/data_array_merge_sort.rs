// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow::array::build_compare;
use common_arrow::arrow::array::make_array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::DynComparator;
use common_arrow::arrow::array::MutableArrayData;
use common_arrow::arrow::compute::SortOptions;

pub fn merge_array(lhs: &ArrayRef, rhs: &ArrayRef, indices: &[bool]) -> Result<ArrayRef> {
    if lhs.data_type() != rhs.data_type() {
        bail!("It is impossible to merge arrays of different data types.")
    }

    if lhs.len() + rhs.len() < indices.len() || indices.is_empty() {
        bail!(
            "It is impossible to merge arrays with overflow indices, {}",
            indices.len()
        )
    }

    let arrays = vec![lhs, rhs]
        .iter()
        .map(|a| a.data_ref())
        .collect::<Vec<_>>();

    let mut mutable = MutableArrayData::new(arrays, false, indices.len());
    let (mut left_next, mut right_next, mut last_is_left) = (0usize, 0usize, indices[0]);

    // tomb value
    let extend_indices = [indices, &[false]].concat();

    for (pos, &is_left) in extend_indices[1..].iter().enumerate() {
        if is_left != last_is_left || pos + 1 == indices.len() {
            if last_is_left {
                mutable.extend(0, left_next, pos + 1 - right_next);
                left_next = pos + 1 - right_next;
            } else {
                mutable.extend(1, right_next, pos + 1 - left_next);
                right_next = pos + 1 - left_next;
            }
            last_is_left = is_left;
        }
    }

    Ok(make_array(mutable.freeze()))
}

/// Given two sets of _ordered_ arrays, returns a bool vector denoting which of the items of the lhs and rhs are to pick from so that
/// if we were to sort-merge the lhs and rhs arrays together, they would all be sorted according to the `options`.
/// # Errors
/// This function errors when:
/// * `lhs.len() != rhs.len()`
/// * `lhs.len() == 0`
/// * `lhs.len() != options.len()`
/// * Arrays on `lhs` and `rhs` have no order relationship
pub fn merge_indices(
    lhs: &[ArrayRef],
    rhs: &[ArrayRef],
    options: &[SortOptions],
    limit: Option<usize>,
) -> Result<Vec<bool>> {
    if lhs.len() != rhs.len() {
        bail!(
            "Merge requires lhs and rhs to have the same number of arrays. lhs has {}, rhs has {}.",
            lhs.len(),
            rhs.len()
        )
    };
    if lhs.is_empty() {
        bail!("Merge requires lhs to have at least 1 entry.")
    };
    if lhs.len() != options.len() {
        bail!("Merge requires the number of sort options to equal number of columns. lhs has {} entries, options has {} entries", lhs.len(), options.len());
    };

    // prepare the comparison function between lhs and rhs arrays
    let cmp = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(l, r)| build_compare(l.as_ref(), r.as_ref()))
        .collect::<common_arrow::arrow::error::Result<Vec<DynComparator>>>()
        .map_err(|e| anyhow!("Build dynComparator error: {:?}", e))?;

    // prepare a comparison function taking into account nulls and sort options
    let cmp = |left, right| {
        for c in 0..lhs.len() {
            let descending = options[c].descending;
            let null_first = options[c].nulls_first;
            let mut result = match (lhs[c].is_valid(left), rhs[c].is_valid(right)) {
                (true, true) => (cmp[c])(left, right),
                (false, true) => {
                    if null_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (true, false) => {
                    if null_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => Ordering::Equal,
            };
            if descending {
                result = result.reverse();
            };
            if result != Ordering::Equal {
                // we found a relevant comparison => short-circuit and return it
                return result;
            }
        }
        Ordering::Equal
    };

    // the actual merge-sort code is from this point onwards
    let mut left = 0; // Head of left pile.
    let mut right = 0; // Head of right pile.
    let max_left = lhs[0].len();
    let max_right = rhs[0].len();

    let limits = match limit {
        Some(limit) => limit.min(max_left + max_right),
        _ => max_left + max_right,
    };

    let mut result = Vec::with_capacity(limits);
    while left < max_left || right < max_right {
        let order = match (left >= max_left, right >= max_right) {
            (true, true) => break,
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => (cmp)(left, right),
        };
        let value = if order == Ordering::Less {
            left += 1;
            true
        } else {
            right += 1;
            false
        };
        result.push(value);
        if result.len() >= limits {
            break;
        }
    }
    Ok(result)
}
