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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::Result;

use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::ValueType;

impl<'a> Selector<'a> {
    // Select indices by comparing scalar and column.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_column_scalar<T: ValueType>(
        &self,
        op: &SelectOp,
        left: T::Column,
        right: T::ScalarRef<'a>,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;

        let false_selection = false_selection.0;

        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;

        let (start, end) = match select_strategy {
            SelectStrategy::True => (*mutable_true_idx, *mutable_true_idx + count),
            SelectStrategy::False => (*mutable_false_idx, *mutable_false_idx + count),
            SelectStrategy::All => (0, count),
        };

        unsafe {
            match validity {
                Some(validity) => {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        let ret = validity.get_bit_unchecked(idx as usize)
                            && op.expect_result(T::compare(
                                T::index_column_unchecked(&left, idx as usize),
                                right.clone(),
                            ));
                        if has_true {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if has_false {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
                None => {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        let ret = op.expect_result(T::compare(
                            T::index_column_unchecked(&left, idx as usize),
                            right.clone(),
                        ));
                        if has_true {
                            *true_selection.get_unchecked_mut(true_idx) = idx;
                            true_idx += ret as usize;
                        }
                        if has_false {
                            *false_selection.get_unchecked_mut(false_idx) = idx;
                            false_idx += !ret as usize;
                        }
                    }
                }
            }
        }

        let true_count = true_idx - *mutable_true_idx;
        let false_count = false_idx - *mutable_false_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        if has_true {
            Ok(true_count)
        } else {
            Ok(count - false_count)
        }
    }
}
