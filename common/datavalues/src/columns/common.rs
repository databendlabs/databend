// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;

use common_arrow::arrow::array::build_compare;
use common_arrow::arrow::array::make_array;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::DynComparator;
use common_arrow::arrow::array::MutableArrayData;
use common_arrow::arrow::compute;
use common_arrow::arrow::compute::SortOptions;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
pub struct DataColumnCommon;

impl DataColumnCommon {
    pub fn concat(columns: &[DataColumn]) -> Result<DataColumn> {
        let arrays = columns
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;

        let dyn_arrays: Vec<&dyn Array> = arrays.iter().map(|arr| arr.as_ref()).collect();

        let array = compute::concat(&dyn_arrays)?;
        Ok(array.into())
    }

    pub fn merge_columns(
        lhs: &DataColumn,
        rhs: &DataColumn,
        indices: &[bool],
    ) -> Result<DataColumn> {
        let lhs = lhs.to_array()?;
        let rhs = rhs.to_array()?;

        let result =
            DataArrayMerge::merge_array(&lhs.get_array_ref(), &rhs.get_array_ref(), indices)?;
        Ok(result.into())
    }

    pub fn merge_indices(
        lhs: &[DataColumn],
        rhs: &[DataColumn],
        options: &[SortOptions],
        limit: Option<usize>,
    ) -> Result<Vec<bool>> {
        let lhs: Vec<ArrayRef> = lhs
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;
        let rhs: Vec<ArrayRef> = rhs
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;

        DataArrayMerge::merge_indices(&lhs, &rhs, options, limit)
    }
}

impl DataColumn {
    #[inline]
    pub fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        let size = self.len();
        let (col, row) = match self {
            DataColumn::Array(array) => (Ok(array.clone()), None),
            DataColumn::Constant(v, _) => (v.to_series_with_size(1), Some(0_usize)),
        };
        let col = col?;

        match col.data_type() {
            DataType::Boolean => {
                let array = col.bool()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&[array.value(row.unwrap_or(i)) as u8]);
                }
            }
            DataType::Float32 => {
                let array = col.f32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Float64 => {
                let array = col.f64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt8 => {
                let array = col.u8()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt16 => {
                let array = col.u16()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt32 => {
                let array = col.u32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt64 => {
                let array = col.u64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int8 => {
                let array = col.i8()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int16 => {
                let array = col.i16()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int32 => {
                let array = col.i32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int64 => {
                let array = col.i64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Utf8 => {
                let array = col.utf8()?.downcast_ref();

                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    let value = array.value(row.unwrap_or(i));
                    // store the size
                    v.extend_from_slice(&value.len().to_le_bytes());
                    // store the string value
                    v.extend_from_slice(value.as_bytes());
                }
            }
            DataType::Date32 => {
                let array = col.date32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }

            _ => {
                // This is internal because we should have caught this before.
                return Result::Err(ErrorCode::BadDataValueType(format!(
                    "Unsupported the col type creating key {}",
                    col.data_type()
                )));
            }
        }
        Ok(())
    }
}

struct DataArrayMerge;

impl DataArrayMerge {
    fn merge_array(lhs: &ArrayRef, rhs: &ArrayRef, indices: &[bool]) -> Result<ArrayRef> {
        if lhs.data_type() != rhs.data_type() {
            return Result::Err(ErrorCode::BadDataValueType(
                "It is impossible to merge arrays of different data types.",
            ));
        }

        if lhs.len() + rhs.len() < indices.len() || indices.is_empty() {
            return Result::Err(ErrorCode::BadDataArrayLength(format!(
                "It is impossible to merge arrays with overflow indices, {}",
                indices.len()
            )));
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
            return Result::Err(ErrorCode::BadDataArrayLength(
                format!(
                    "Merge requires lhs and rhs to have the same number of arrays. lhs has {}, rhs has {}.",
                    lhs.len(),
                    rhs.len()
                )
            ));
        };
        if lhs.is_empty() {
            return Result::Err(ErrorCode::BadDataArrayLength(
                "Merge requires lhs to have at least 1 entry.",
            ));
        };
        if lhs.len() != options.len() {
            return Result::Err(ErrorCode::BadDataArrayLength(
                format!(
                    "Merge requires the number of sort options to equal number of columns. lhs has {} entries, options has {} entries",
                    lhs.len(),
                    options.len()
                )
            ));
        };

        // prepare the comparison function between lhs and rhs arrays
        let cmp = lhs
            .iter()
            .zip(rhs.iter())
            .map(|(l, r)| build_compare(l.as_ref(), r.as_ref()))
            .collect::<common_arrow::arrow::error::Result<Vec<DynComparator>>>()?;

        // prepare a comparison function taking into account nulls and sort options
        let cmp = |left, right| {
            let _ = (&lhs, &options, &rhs);
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
}
