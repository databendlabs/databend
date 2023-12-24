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
    pub(crate) fn select_type_values<T: ValueType>(
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
        match (left, right) {
            (Value::Column(a), Value::Column(b)) => {
                let a = T::try_downcast_column(&a).unwrap();
                let b = T::try_downcast_column(&b).unwrap();

                self.select_columns::<T>(
                    op,
                    a,
                    b,
                    validity,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            (Value::Column(a), Value::Scalar(b)) => {
                let a = T::try_downcast_column(&a).unwrap();
                let b = b.as_ref();
                let b = T::try_downcast_scalar(&b).unwrap();

                self.select_column_scalar::<T>(
                    op,
                    a,
                    b,
                    validity,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                )
            }
            _ => unreachable!(),
        }
    }
}
