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

use crate::arrow::and_validities;
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
use crate::Column;
use crate::LikePattern;
use crate::Scalar;
use crate::Selector;
use crate::Value;

impl<'a> Selector<'a> {
    // Select indices by comparing two `Value`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_values(
        &self,
        op: &SelectOp,
        mut left: Value<AnyType>,
        mut right: Value<AnyType>,
        left_data_type: DataType,
        right_data_type: DataType,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        // Check if the left or right is `Scalar::Null`.
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

        // Remove `NullableColumn` and get the inner column and validity.
        let mut validity = None;
        if let (Value::Column(column), DataType::Nullable(_)) = (&left, &left_data_type) {
            let nullable_column = column.clone().into_nullable().unwrap();
            left = Value::Column(nullable_column.column);
            validity = Some(nullable_column.validity);
        }
        if let (Value::Column(column), DataType::Nullable(_)) = (&right, &right_data_type) {
            let nullable_column = column.clone().into_nullable().unwrap();
            right = Value::Column(nullable_column.column);
            validity = and_validities(Some(nullable_column.validity), validity);
        }

        let op = if let (Value::Scalar(_), Value::Column(_)) = (&left, &right) {
            op.reverse()
        } else {
            op.clone()
        };

        match left_data_type.remove_nullable() {
            DataType::Number(ty) => {
                with_number_mapped_type!(|T| match ty {
                    NumberDataType::T => self.select_type_values_cmp::<NumberType<T>>(
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
                    DecimalDataType::T(_) => self.select_type_values_cmp::<DecimalType<T>>(
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
            DataType::Date => self.select_type_values_cmp::<DateType>(
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
            DataType::Timestamp => self.select_type_values_cmp::<TimestampType>(
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
            DataType::String => self.select_type_values_cmp::<StringType>(
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
            DataType::Variant => self.select_type_values_cmp::<VariantType>(
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
            DataType::Boolean => self.select_type_values_cmp::<BooleanType>(
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
            DataType::EmptyArray => self.select_type_values_cmp::<EmptyArrayType>(
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
            _ => self.select_type_values_cmp::<AnyType>(
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_like(
        &self,
        mut column: Column,
        data_type: &DataType,
        like_pattern: &LikePattern,
        like_str: &[u8],
        not: bool,
        true_selection: &mut [u32],
        false_selection: (&mut [u32], bool),
        mutable_true_idx: &mut usize,
        mutable_false_idx: &mut usize,
        select_strategy: SelectStrategy,
        count: usize,
    ) -> Result<usize> {
        // Remove `NullableColumn` and get the inner column and validity.
        let mut validity = None;
        if let DataType::Nullable(_) = data_type {
            let nullable_column = column.clone().into_nullable().unwrap();
            column = nullable_column.column;
            validity = Some(nullable_column.validity);
        }
        // It's safe to unwrap because the column's data type is `DataType::String`.
        let column = column.into_string().unwrap();

        // To unite the function signature, we define a dummy function for `LikePattern::SimplePattern`.
        let dummy_function = |_: &[u8], _: &[u8]| -> bool { false };
        let cmp = match like_pattern {
            LikePattern::OrdinalStr => LikePattern::ordinal_str,
            LikePattern::StartOfPercent => LikePattern::start_of_percent,
            LikePattern::EndOfPercent => LikePattern::end_of_percent,
            LikePattern::SurroundByPercent => LikePattern::surround_by_percent,
            LikePattern::ComplexPattern => LikePattern::complex_pattern,
            _ => dummy_function,
        };

        self.select_like_adapt::<_>(
            cmp,
            column,
            like_pattern,
            like_str,
            not,
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
