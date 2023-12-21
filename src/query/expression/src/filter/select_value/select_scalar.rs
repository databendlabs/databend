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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::filter::empty_array_compare_value;
use crate::filter::select_op;
use crate::filter::select_op_tuple;
use crate::filter::tuple_compare_default_value;
use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::filter::Selector;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::Scalar;

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
        if left == Scalar::Null || right == Scalar::Null {
            if false_selection.1 {
                return Ok(self.select_null(
                    true_selection,
                    false_selection.0,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                ));
            } else {
                return Ok(0);
            }
        }

        let result = match data_type.remove_nullable() {
            DataType::Null | DataType::EmptyMap => false,
            DataType::EmptyArray => empty_array_compare_value(op),
            DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Date
            | DataType::Timestamp
            | DataType::Array(_) => select_op(op)(left, right),
            DataType::Boolean => {
                let left = left.into_boolean().unwrap();
                let right = right.into_boolean().unwrap();
                BooleanType::cmp(op)(left, right)
            }
            DataType::String => {
                let left = left.into_string().unwrap();
                let right = right.into_string().unwrap();
                StringType::cmp(op)(&left, &right)
            }
            DataType::Variant => {
                let left = left.into_variant().unwrap();
                let right = right.into_variant().unwrap();
                VariantType::cmp(op)(&left, &right)
            }
            DataType::Tuple(_) => {
                let left = left.into_tuple().unwrap();
                let right = right.into_tuple().unwrap();
                let mut ret = tuple_compare_default_value(op);
                let op = select_op_tuple::<Scalar>(op);
                for (lhs, rhs) in left.into_iter().zip(right.into_iter()) {
                    if let Some(result) = op(lhs, rhs) {
                        ret = result;
                        break;
                    }
                }
                ret
            }
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

    pub(crate) fn select_null(
        &self,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let mut false_idx = *mutable_false_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                for i in start..end {
                    let idx = *true_selection.get_unchecked(i);
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += 1;
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                for i in start..end {
                    let idx = *false_selection.get_unchecked(i);
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += 1;
                }
            },
            SelectStrategy::All => unsafe {
                for idx in 0u32..count as u32 {
                    *false_selection.get_unchecked_mut(false_idx) = idx;
                    false_idx += 1;
                }
            },
        }
        *mutable_false_idx = false_idx;
        0
    }

    pub fn select_boolean_scalar_adapt(
        &self,
        scalar: bool,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;
        if has_true && has_false {
            self.select_boolean_scalar::<true, true>(
                scalar,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_boolean_scalar::<true, false>(
                scalar,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean_scalar::<false, true>(
                scalar,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        }
    }

    fn select_boolean_scalar<const TRUE: bool, const FALSE: bool>(
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

    #[allow(clippy::too_many_arguments)]
    pub fn select_empty_array_adapt(
        &self,
        op: &SelectOp,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let has_true = !true_selection.is_empty();
        let has_false = false_selection.1;
        if has_true && has_false {
            self.select_empty_array::<true, true>(
                op,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_empty_array::<true, false>(
                op,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_empty_array::<false, true>(
                op,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                false_idx,
                select_strategy,
                count,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn select_empty_array<const TRUE: bool, const FALSE: bool>(
        &self,
        op: &SelectOp,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> usize {
        let ret = empty_array_compare_value(op);
        let mut true_idx = *mutable_true_idx;
        let mut false_idx = *mutable_false_idx;
        match select_strategy {
            SelectStrategy::True => unsafe {
                let start = *mutable_true_idx;
                let end = *mutable_true_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            let ret = ret & validity.get_bit_unchecked(idx as usize);
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *true_selection.get_unchecked(i);
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::False => unsafe {
                let start = *mutable_false_idx;
                let end = *mutable_false_idx + count;
                match validity {
                    Some(validity) => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            let ret = ret & validity.get_bit_unchecked(idx as usize);
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for i in start..end {
                            let idx = *false_selection.get_unchecked(i);
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
            SelectStrategy::All => unsafe {
                match validity {
                    Some(validity) => {
                        for idx in 0u32..count as u32 {
                            let ret = ret & validity.get_bit_unchecked(idx as usize);
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                    None => {
                        for idx in 0u32..count as u32 {
                            if TRUE {
                                *true_selection.get_unchecked_mut(true_idx) = idx;
                                true_idx += ret as usize;
                            }
                            if FALSE {
                                *false_selection.get_unchecked_mut(false_idx) = idx;
                                false_idx += !ret as usize;
                            }
                        }
                    }
                }
            },
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
