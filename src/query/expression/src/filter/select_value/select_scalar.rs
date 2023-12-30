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

use databend_common_exception::Result;

use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::ValueType;
use crate::Scalar;
use crate::SelectOp;

impl<'a> Selector<'a> {
    #[allow(clippy::too_many_arguments)]
    // Select indices by comparing two scalars.
    pub(crate) fn select_scalars<T: ValueType>(
        &self,
        op: &SelectOp,
        left: Scalar,
        right: Scalar,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let left = left.as_ref();
        let left = T::try_downcast_scalar(&left).unwrap();
        let left = T::to_owned_scalar(left);
        let left = T::scalar_to_compare_key(&left).unwrap();

        let right = right.as_ref();
        let right = T::try_downcast_scalar(&right).unwrap();
        let right = T::to_owned_scalar(right);
        let right = T::scalar_to_compare_key(&right).unwrap();

        let result = T::compare_operation(op)(left, right);
        let count = self.select_boolean_scalar_adapt(
            result,
            true_selection,
            false_selection,
            mutable_true_idx,
            mutable_false_idx,
            select_strategy,
            count,
        );
        Ok(count)
    }

    pub(crate) fn select_boolean_scalar<const FALSE: bool>(
        &self,
        scalar: bool,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                if scalar {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    for i in start..end {
                        let idx = *true_selection.get_unchecked(i);
                        false_selection[false_idx] = idx;
                        false_idx += 1;
                    }
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                if scalar {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    for i in start..end {
                        let idx = *false_selection.get_unchecked(i);
                        false_selection[false_idx] = idx;
                        false_idx += 1;
                    }
                }
            },
            SelectStrategy::All => {
                if scalar {
                    for idx in 0u32..count as u32 {
                        true_selection[true_idx] = idx;
                        true_idx += 1;
                    }
                } else if FALSE {
                    for idx in 0u32..count as u32 {
                        false_selection[false_idx] = idx;
                        false_idx += 1;
                    }
                }
            }
        }
        let true_count = true_idx - *mutable_true_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        true_count
    }
}
