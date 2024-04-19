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

use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ValueType;
use crate::with_mapped_cmp_method;
use crate::SelectOp;
use crate::SelectStrategy;
use crate::Selector;
use crate::Value;

mod select_column;
mod select_column_scalar;
mod select_scalar;

use crate::LikePattern;

impl<'a> Selector<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_type_values_cmp<T: ValueType>(
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
        with_mapped_cmp_method!(|OP| match op {
            SelectOp::OP => self.select_type_values::<T, _>(
                T::OP,
                left,
                right,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_type_values<
        T: ValueType,
        C: Fn(T::ScalarRef<'_>, T::ScalarRef<'_>) -> bool,
    >(
        &self,
        cmp: C,
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
        let has_false = false_selection.1;
        match (left, right) {
            (Value::Scalar(left), Value::Scalar(right)) => self.select_scalars::<T, C>(
                cmp,
                left,
                right,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            (Value::Column(left), Value::Column(right)) => {
                let left = T::try_downcast_column(&left).unwrap();
                let right = T::try_downcast_column(&right).unwrap();

                if has_false {
                    self.select_columns::<T, C, true>(
                        cmp,
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
                    self.select_columns::<T, C, false>(
                        cmp,
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
            (Value::Column(column), Value::Scalar(scalar))
            | (Value::Scalar(scalar), Value::Column(column)) => {
                let column = T::try_downcast_column(&column).unwrap();
                let scalar = scalar.as_ref();
                let scalar = T::try_downcast_scalar(&scalar).unwrap();

                if has_false {
                    self.select_column_scalar::<T, C, true>(
                        cmp,
                        column,
                        scalar,
                        validity,
                        true_selection,
                        false_selection.0,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    )
                } else {
                    self.select_column_scalar::<T, C, false>(
                        cmp,
                        column,
                        scalar,
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
        let has_false = false_selection.1;
        if has_false {
            self.select_boolean_scalar::<true>(
                scalar,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean_scalar::<false>(
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
        let has_false = false_selection.1;
        if has_false {
            self.select_boolean_column::<true>(
                column,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_boolean_column::<false>(
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_like_adapt<C: Fn(&[u8], &[u8]) -> bool>(
        &self,
        cmp: C,
        column: StringColumn,
        like_pattern: &LikePattern,
        like_str: &[u8],
        not: bool,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        let has_false = false_selection.1;
        let is_simple_pattern = matches!(like_pattern, LikePattern::SimplePattern(_));
        match (has_false, is_simple_pattern) {
            (true, true) => self.select_like_not::<_, true, true>(
                cmp,
                column,
                like_pattern,
                like_str,
                not,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            (true, false) => self.select_like_not::<_, true, false>(
                cmp,
                column,
                like_pattern,
                like_str,
                not,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            (false, true) => self.select_like_not::<_, false, true>(
                cmp,
                column,
                like_pattern,
                like_str,
                not,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            (false, false) => self.select_like_not::<_, false, false>(
                cmp,
                column,
                like_pattern,
                like_str,
                not,
                validity,
                true_selection,
                false_selection.0,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_like_not<
        C: Fn(&[u8], &[u8]) -> bool,
        const FALSE: bool,
        const SIMPLE_PATTERN: bool,
    >(
        &self,
        cmp: C,
        column: StringColumn,
        like_pattern: &LikePattern,
        like_str: &[u8],
        not: bool,
        validity: Option<Bitmap>,
        true_selection: &mut [u32],
        false_selection: &mut [u32],
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        if not {
            self.select_column_like::<_, FALSE, SIMPLE_PATTERN, true>(
                cmp,
                column,
                like_pattern,
                like_str,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        } else {
            self.select_column_like::<_, FALSE, SIMPLE_PATTERN, false>(
                cmp,
                column,
                like_pattern,
                like_str,
                validity,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            )
        }
    }
}
