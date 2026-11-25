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

use databend_common_column::binview::ViewProbe;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;

use super::SelectionBuffers;
use crate::SelectOp;
use crate::Value;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::StringType;
use crate::types::string::StringColumn;

impl Selector<'_> {
    /// Selects by a string comparison, deciding from the string views where they can:
    /// equality rejects a row by its length and prefix and settles an inlined value without
    /// reading the data buffers, ordering decides from differing prefixes. `op` is oriented as
    /// `<column> <op> <scalar>`.
    pub(in crate::filter) fn select_string_values(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        match (left, right) {
            (Value::Column(left), Value::Column(right)) => {
                let left = StringType::try_downcast_column(&left).unwrap();
                let right = StringType::try_downcast_column(&right).unwrap();
                Ok(self.select_string_rows(
                    op,
                    |row| StringColumn::equal(&left, row, &right, row),
                    |row| StringColumn::compare(&left, row, &right, row),
                    validity,
                    buffers,
                    has_false,
                ))
            }
            (Value::Column(column), Value::Scalar(scalar))
            | (Value::Scalar(scalar), Value::Column(column)) => {
                let column = StringType::try_downcast_column(&column).unwrap();
                let scalar = scalar.as_ref();
                let scalar = StringType::try_downcast_scalar(&scalar).unwrap();
                let probe = ViewProbe::new(scalar.as_bytes());
                Ok(self.select_string_rows(
                    op,
                    |row| column.equal_probe(row, &probe),
                    |row| column.compare_probe(row, &probe),
                    validity,
                    buffers,
                    has_false,
                ))
            }
            (left, right) => self.select_type_values_cmp::<StringType>(
                op, left, right, validity, buffers, has_false,
            ),
        }
    }

    /// Selects the rows that satisfy `op`, given whether a row equals the other side and how it
    /// orders against it.
    fn select_string_rows(
        &self,
        op: &SelectOp,
        equal: impl Fn(usize) -> bool,
        compare: impl Fn(usize) -> Ordering,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> usize {
        match op {
            SelectOp::Equal => self.select_rows_adapt(equal, validity, buffers, has_false),
            SelectOp::NotEqual => {
                self.select_rows_adapt(|row| !equal(row), validity, buffers, has_false)
            }
            SelectOp::Gt => self.select_rows_adapt(
                |row| compare(row) == Ordering::Greater,
                validity,
                buffers,
                has_false,
            ),
            SelectOp::Lt => self.select_rows_adapt(
                |row| compare(row) == Ordering::Less,
                validity,
                buffers,
                has_false,
            ),
            SelectOp::Gte => self.select_rows_adapt(
                |row| compare(row) != Ordering::Less,
                validity,
                buffers,
                has_false,
            ),
            SelectOp::Lte => self.select_rows_adapt(
                |row| compare(row) != Ordering::Greater,
                validity,
                buffers,
                has_false,
            ),
        }
    }

    fn select_rows_adapt(
        &self,
        predicate: impl Fn(usize) -> bool,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> usize {
        match (validity, has_false) {
            (Some(validity), true) => self.select_rows::<true>(
                |row| unsafe { validity.get_bit_unchecked(row) } && predicate(row),
                buffers,
            ),
            (Some(validity), false) => self.select_rows::<false>(
                |row| unsafe { validity.get_bit_unchecked(row) } && predicate(row),
                buffers,
            ),
            (None, true) => self.select_rows::<true>(predicate, buffers),
            (None, false) => self.select_rows::<false>(predicate, buffers),
        }
    }

    /// Selects the rows for which `predicate` holds, with the same use of the selection buffers
    /// as [`Self::select_column_scalar`].
    fn select_rows<const FALSE: bool>(
        &self,
        predicate: impl Fn(usize) -> bool,
        buffers: SelectionBuffers,
    ) -> usize {
        let SelectionBuffers {
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        } = buffers;

        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;

        let mut update_index =
            |idx: u32, true_selection: &mut [u32], false_selection: &mut [u32]| unsafe {
                let ret = predicate(idx as usize);
                *true_selection.get_unchecked_mut(true_idx) = idx;
                true_idx += ret as usize;
                if FALSE {
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += !ret as usize;
                }
            };

        match select_strategy {
            SelectStrategy::True => {
                let start = *mutable_true_idx;
                for i in start..start + count {
                    let idx = unsafe { *true_selection.get_unchecked(i) };
                    update_index(idx, true_selection, false_selection);
                }
            }
            SelectStrategy::False => {
                let start = *mutable_false_idx;
                for i in start..start + count {
                    let idx = unsafe { *false_selection.get_unchecked(i) };
                    update_index(idx, true_selection, false_selection);
                }
            }
            SelectStrategy::All => {
                for idx in 0..count as u32 {
                    update_index(idx, true_selection, false_selection);
                }
            }
        }

        let true_count = true_idx - *mutable_true_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        true_count
    }
}
