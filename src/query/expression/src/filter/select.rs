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
use ethnum::i256;

use crate::filter::SelectOp;
use crate::filter::SelectStrategy;
use crate::types::decimal::DecimalType;
use crate::types::nullable::NullableColumn;
use crate::types::number::*;
use crate::types::AnyType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataType;
use crate::types::EmptyArrayType;
use crate::types::NullableType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::VariantType;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Scalar;
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
        if Value::Scalar(Scalar::Null) == left || Value::Scalar(Scalar::Null) == right {
            if false_selection.1 {
                return Ok(self.select_boolean_scalar_adapt(
                    false,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                ));
            } else {
                return Ok(0);
            }
        }

        match (left, right) {
            // Select indices by comparing two scalars.
            (Value::Scalar(left), Value::Scalar(right)) => {
                let result = op.expect()(left.cmp(&right));
                Ok(self.select_boolean_scalar_adapt(
                    result,
                    true_selection,
                    false_selection,
                    mutable_true_idx,
                    mutable_false_idx,
                    select_strategy,
                    count,
                ))
            }

            (left, right) => {
                let mut op = op.clone();
                let (left, right, validity, data_type) = match (left, right) {
                    (Value::Column(left), Value::Column(right)) => {
                        debug_assert!(left_data_type == right_data_type);
                        if left_data_type.is_nullable() {
                            let left = left.into_nullable().unwrap();
                            let right = right.into_nullable().unwrap();
                            let validity = (&left.validity) & (&right.validity);
                            (
                                Value::<AnyType>::Column(left.column),
                                Value::<AnyType>::Column(right.column),
                                Some(validity),
                                left_data_type.remove_nullable(),
                            )
                        } else {
                            (
                                Value::Column(left),
                                Value::Column(right),
                                None,
                                left_data_type,
                            )
                        }
                    }
                    (Value::Column(column), scalar) => {
                        if left_data_type.is_nullable() {
                            let column = column.into_nullable().unwrap();
                            let validity = column.validity.clone();
                            (
                                Value::Column(column.column),
                                scalar,
                                Some(validity),
                                left_data_type.remove_nullable(),
                            )
                        } else {
                            (Value::Column(column), scalar, None, left_data_type)
                        }
                    }
                    (scalar, Value::Column(column)) => {
                        op = op.reverse();
                        if right_data_type.is_nullable() {
                            let column = column.into_nullable().unwrap();
                            let validity = column.validity.clone();
                            (
                                Value::Column(column.column),
                                scalar,
                                Some(validity),
                                right_data_type.remove_nullable(),
                            )
                        } else {
                            (Value::Column(column), scalar, None, right_data_type)
                        }
                    }
                    _ => unreachable!(),
                };

                match data_type {
                    DataType::Number(ty) => {
                        with_number_mapped_type!(|T| match ty {
                            NumberDataType::T => self.select_type_values::<NumberType<T>, false>(
                                &op,
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

                    DataType::Decimal(ty) => {
                        with_decimal_mapped_type!(|T| match ty {
                            DecimalDataType::T(_) => self
                                .select_type_values::<DecimalType<T>, false>(
                                    &op,
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
                    DataType::Date => self.select_type_values::<DateType, false>(
                        &op,
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
                    DataType::Timestamp => self.select_type_values::<TimestampType, false>(
                        &op,
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
                    DataType::String => self.select_type_values::<StringType, false>(
                        &op,
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
                    DataType::Variant => self.select_type_values::<VariantType, false>(
                        &op,
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
                    DataType::Boolean => self.select_type_values::<BooleanType, false>(
                        &op,
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
                    DataType::EmptyArray => self.select_type_values::<EmptyArrayType, false>(
                        &op,
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
                    _ => self.select_type_values::<AnyType, true>(
                        &op,
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
                }
            }
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
