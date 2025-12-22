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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;

use super::SelectionBuffers;
use crate::LikePattern;
use crate::SelectOp;
use crate::Selector;
use crate::Value;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::string::StringColumn;
use crate::with_mapped_cmp_method;

mod select_column;
mod select_column_scalar;
mod select_scalar;

impl Selector<'_> {
    pub(super) fn select_type_values_cmp<T: AccessType>(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        self.select_type_values_cmp_lr::<T, T>(op, left, right, validity, buffers, has_false)
    }

    pub(super) fn select_type_values_cmp_lr<L, R>(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize>
    where
        L: AccessType,
        R: for<'a> AccessType<ScalarRef<'a> = L::ScalarRef<'a>>,
    {
        with_mapped_cmp_method!(|OP| match op {
            SelectOp::OP => self.select_type_values::<L, R, _>(
                L::OP,
                left,
                right,
                validity,
                buffers,
                has_false,
            ),
        })
    }

    fn select_type_values<L, R, C>(
        &self,
        cmp: C,
        left: Value<AnyType>,
        right: Value<AnyType>,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize>
    where
        L: AccessType,
        R: for<'a> AccessType<ScalarRef<'a> = L::ScalarRef<'a>>,
        C: Fn(L::ScalarRef<'_>, L::ScalarRef<'_>) -> bool,
    {
        match (left, right) {
            (Value::Scalar(left), Value::Scalar(right)) => {
                let left = left.as_ref();
                let left = L::try_downcast_scalar(&left).unwrap();
                let right = right.as_ref();
                let right = R::try_downcast_scalar(&right).unwrap();

                let result = cmp(left, right);
                let count = self.select_boolean_scalar_adapt(result, buffers, has_false);
                Ok(count)
            }
            (Value::Column(left), Value::Column(right)) => {
                let left = L::try_downcast_column(&left).unwrap();
                let right = R::try_downcast_column(&right).unwrap();

                if has_false {
                    self.select_columns::<true, L, R, _>(cmp, left, right, validity, buffers)
                } else {
                    self.select_columns::<false, L, R, _>(cmp, left, right, validity, buffers)
                }
            }
            (Value::Column(column), Value::Scalar(scalar)) => {
                let column = L::try_downcast_column(&column).unwrap();
                let scalar = scalar.as_ref();
                let scalar = R::try_downcast_scalar(&scalar).unwrap();

                if has_false {
                    self.select_column_scalar::<true, L, R, _>(
                        cmp, column, scalar, validity, buffers,
                    )
                } else {
                    self.select_column_scalar::<false, L, R, _>(
                        cmp, column, scalar, validity, buffers,
                    )
                }
            }
            (Value::Scalar(scalar), Value::Column(column)) => {
                let column = R::try_downcast_column(&column).unwrap();
                let scalar = scalar.as_ref();
                let scalar = L::try_downcast_scalar(&scalar).unwrap();

                if has_false {
                    self.select_column_scalar::<true, R, L, _>(
                        cmp, column, scalar, validity, buffers,
                    )
                } else {
                    self.select_column_scalar::<false, R, L, _>(
                        cmp, column, scalar, validity, buffers,
                    )
                }
            }
        }
    }

    pub(super) fn select_boolean_scalar_adapt(
        &self,
        scalar: bool,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> usize {
        if has_false {
            self.select_boolean_scalar::<true>(scalar, buffers)
        } else {
            self.select_boolean_scalar::<false>(scalar, buffers)
        }
    }

    pub(super) fn select_boolean_column_adapt(
        &self,
        column: Bitmap,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> usize {
        if has_false {
            self.select_boolean_column::<true>(column, buffers)
        } else {
            self.select_boolean_column::<false>(column, buffers)
        }
    }

    pub(super) fn select_like(
        &self,
        column: StringColumn,
        like_pattern: &LikePattern,
        not: bool,
        validity: Option<Bitmap>,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        match (has_false, not) {
            (true, true) => self.select_column_like::<true, true>(
                column,
                like_pattern,
                validity.clone(),
                buffers,
            ),
            (true, false) => self.select_column_like::<true, false>(
                column,
                like_pattern,
                validity.clone(),
                buffers,
            ),
            (false, true) => self.select_column_like::<false, true>(
                column,
                like_pattern,
                validity.clone(),
                buffers,
            ),
            (false, false) => self.select_column_like::<false, false>(
                column,
                like_pattern,
                validity.clone(),
                buffers,
            ),
        }
    }
}
