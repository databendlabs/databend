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

use super::SelectionBuffers;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::AccessType;
use crate::Scalar;

impl Selector<'_> {
    // Select indices by comparing two scalars.
    pub(super) fn select_scalars<
        T: AccessType,
        C: Fn(T::ScalarRef<'_>, T::ScalarRef<'_>) -> bool,
    >(
        &self,
        cmp: C,
        left: Scalar,
        right: Scalar,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        let left = left.as_ref();
        let left = T::try_downcast_scalar(&left).unwrap();
        let right = right.as_ref();
        let right = T::try_downcast_scalar(&right).unwrap();
        let result = cmp(left, right);

        let count = self.select_boolean_scalar_adapt(result, buffers, has_false);
        Ok(count)
    }

    pub(super) fn select_boolean_scalar<const FALSE: bool>(
        &self,
        scalar: bool,
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
