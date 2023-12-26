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

use crate::types::AnyType;
use crate::types::ValueType;
use crate::SelectOp;
use crate::SelectStrategy;
use crate::Selector;
use crate::Value;

mod select_column;
mod select_column_scalar;
mod select_scalar;

impl<'a> Selector<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_type_values<T: ValueType, const IS_ANY_TYPE: bool>(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
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

        match (left, right) {
            (Value::Column(left), Value::Column(right)) => {
                let left = T::try_downcast_column(&left).unwrap();
                let right = T::try_downcast_column(&right).unwrap();

                if has_true && has_false {
                    self.select_columns::<T, true, true, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                } else if has_true {
                    self.select_columns::<T, true, false, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                } else {
                    self.select_columns::<T, false, true, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                }
            }
            (Value::Column(left), Value::Scalar(right)) => {
                let left = T::try_downcast_column(&left).unwrap();
                let right = right.as_ref();
                let right = T::try_downcast_scalar(&right).unwrap();

                if has_true && has_false {
                    self.select_column_scalar::<T, true, true, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                } else if has_true {
                    self.select_column_scalar::<T, true, false, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                } else {
                    self.select_column_scalar::<T, false, true, IS_ANY_TYPE>(
                        op,
                        left,
                        right,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn select_boolean_scalar_adapt(
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

    pub(crate) fn select_boolean_column_adapt(
        &self,
        column: Bitmap,
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
            self.select_boolean_column::<true, true>(
                column,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else if has_true {
            self.select_boolean_column::<true, false>(
                column,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean_column::<false, true>(
                column,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        }
    }
}
