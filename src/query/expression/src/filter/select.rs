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

use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::types::nullable::NullableColumn;
use crate::types::AnyType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::NullableType;
use crate::Selector;
use crate::Value;

impl<'a> Selector<'a> {
    // Select indices by comparing two `Value`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_values(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        left_data_type: DataType,
        right_data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        match (left, right) {
            // Select indices by comparing two scalars.
            (Value::Scalar(left), Value::Scalar(right)) => self.select_scalars(
                op,
                left,
                right,
                left_data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            // Select indices by comparing two columns.
            (Value::Column(left), Value::Column(right)) => self.select_columns(
                op,
                left,
                right,
                left_data_type,
                right_data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            // Select indices by comparing scalar and column.
            (Value::Scalar(scalar), Value::Column(column)) => self.select_scalar_and_column(
                op,
                scalar,
                column,
                right_data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
            // Select indices by comparing column and scalar.
            (Value::Column(column), Value::Scalar(scalar)) => self.select_scalar_and_column(
                &op.reverse(),
                scalar,
                column,
                left_data_type,
                true_selection,
                false_selection,
                mutable_true_idx,
                mutable_false_idx,
                select_strategy,
                count,
            ),
        }
    }

    // Select indices by single `Value`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_value(
        &self,
        value: Value<AnyType>,
        data_type: &DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        debug_assert!(
            matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean))
        );

        let count = match data_type {
            DataType::Boolean => {
                let value = value.try_downcast::<BooleanType>().unwrap();
                match value {
                    Value::Scalar(scalar) => self.select_boolean_scalar_adapt(
                        scalar,
                        true_selection,
                        false_selection,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Column(column) => self.select_boolean_column_adapt(
                        column,
                        true_selection,
                        false_selection,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    ),
                }
            }
            DataType::Nullable(box DataType::Boolean) => {
                let nullable_value = value.try_downcast::<NullableType<BooleanType>>().unwrap();
                match nullable_value {
                    Value::Scalar(None) => self.select_boolean_scalar_adapt(
                        false,
                        true_selection,
                        false_selection,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Scalar(Some(scalar)) => self.select_boolean_scalar_adapt(
                        scalar,
                        true_selection,
                        false_selection,
                        mutable_true_idx,
                        mutable_false_idx,
                        select_strategy,
                        count,
                    ),
                    Value::Column(NullableColumn { column, validity }) => {
                        let bitmap = &column & &validity;
                        self.select_boolean_column_adapt(
                            bitmap,
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
            _ => {
                return Err(ErrorCode::UnsupportedDataType(format!(
                    "Filtering by single Value only supports Boolean or Nullable(Boolean), but getting {:?}",
                    &data_type
                )));
            }
        };
        Ok(count)
    }
}
