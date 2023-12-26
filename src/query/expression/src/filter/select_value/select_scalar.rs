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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::ValueType;
use crate::Scalar;
use crate::SelectOp;

impl<'a> Selector<'a> {
    #[allow(clippy::too_many_arguments)]
    // Select indices by comparing two scalars.
    pub(crate) fn select_scalars(
        &self,
        op: &SelectOp,
        left: Scalar,
        right: Scalar,
        data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let result = match data_type.remove_nullable() {
            DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Date
            | DataType::Timestamp
            | DataType::String
            | DataType::Variant
            | DataType::EmptyArray
            | DataType::Tuple(_)
            | DataType::Array(_) => op.expect()(left.cmp(&right)),
            DataType::Boolean => BooleanType::compare_operation(op)(
                left.into_boolean().unwrap(),
                right.into_boolean().unwrap(),
            ),
            _ => {
                // EmptyMap, Map, Bitmap do not support comparison, Nullable has been removed,
                // Generic has been converted to a specific DataType.
                return Err(ErrorCode::UnsupportedDataType(format!(
                    "{:?} is not supported for comparison",
                    &data_type
                )));
            }
        };

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

    pub(crate) fn select_boolean_scalar<const TRUE: bool, const FALSE: bool>(
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
                    if TRUE {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            true_selection[true_idx] = idx;
                            true_idx += 1;
                        }
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
                    if TRUE {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            true_selection[true_idx] = idx;
                            true_idx += 1;
                        }
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
                    if TRUE {
                        for idx in 0u32..count as u32 {
                            true_selection[true_idx] = idx;
                            true_idx += 1;
                        }
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
        let false_count = false_idx - *mutable_false_idx;
        *mutable_true_idx = true_idx;
        *mutable_false_idx = false_idx;
        if TRUE {
            true_count
        } else {
            count - false_count
        }
    }
}
