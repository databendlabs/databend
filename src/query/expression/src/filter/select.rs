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

use super::SelectionBuffers;
use crate::Selector;
use crate::Value;
use crate::filter::SelectOp;
use crate::types::AnyType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::Decimal64As128Type;
use crate::types::Decimal64As256Type;
use crate::types::Decimal64Type;
use crate::types::Decimal128As64Type;
use crate::types::Decimal128As256Type;
use crate::types::Decimal128Type;
use crate::types::Decimal256As64Type;
use crate::types::Decimal256As128Type;
use crate::types::Decimal256Type;
use crate::types::DecimalDataKind;
use crate::types::DecimalDataType;
use crate::types::EmptyArrayType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::VariantType;
use crate::types::number::*;
use crate::types::timestamp_tz::TimestampTzType;
use crate::utils::filter_helper::FilterHelpers;
use crate::with_number_mapped_type;

impl Selector<'_> {
    // Select indices by comparing two `Value`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_values(
        &self,
        op: &SelectOp,
        mut left: Value<AnyType>,
        mut right: Value<AnyType>,
        left_data_type: DataType,
        right_data_type: DataType,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        // Check if the left or right is `Scalar::Null`.
        if left.is_scalar_null() || right.is_scalar_null() {
            if has_false {
                return Ok(self.select_boolean_scalar_adapt(false, buffers, has_false));
            } else {
                return Ok(0);
            }
        }

        let (left, right, validity) =
            FilterHelpers::normalize_compare_values(left, &left_data_type, right, &right_data_type);

        let op = if let (Value::Scalar(_), Value::Column(_)) = (&left, &right) {
            op.reverse()
        } else {
            op.clone()
        };

        match left_data_type.remove_nullable() {
            DataType::Number(ty) => {
                with_number_mapped_type!(|T| match ty {
                    NumberDataType::T => {
                        self.select_type_values_cmp::<NumberType<T>>(
                            &op, left, right, validity, buffers, has_false,
                        )
                    }
                })
            }

            DataType::Decimal(_) => {
                self.select_values_decimal(&op, left, right, buffers, has_false, validity)
            }
            DataType::Date => self
                .select_type_values_cmp::<DateType>(&op, left, right, validity, buffers, has_false),
            DataType::Timestamp => self.select_type_values_cmp::<TimestampType>(
                &op, left, right, validity, buffers, has_false,
            ),
            DataType::TimestampTz => self.select_type_values_cmp::<TimestampTzType>(
                &op, left, right, validity, buffers, has_false,
            ),
            DataType::String => self.select_type_values_cmp::<StringType>(
                &op, left, right, validity, buffers, has_false,
            ),
            DataType::Variant => self.select_type_values_cmp::<VariantType>(
                &op, left, right, validity, buffers, has_false,
            ),
            DataType::Boolean => self.select_type_values_cmp::<BooleanType>(
                &op, left, right, validity, buffers, has_false,
            ),
            DataType::EmptyArray => self.select_type_values_cmp::<EmptyArrayType>(
                &op, left, right, validity, buffers, has_false,
            ),
            _ => self
                .select_type_values_cmp::<AnyType>(&op, left, right, validity, buffers, has_false),
        }
    }

    fn select_values_decimal(
        &self,
        op: &SelectOp,
        left: Value<AnyType>,
        right: Value<AnyType>,
        buffers: SelectionBuffers<'_>,
        has_false: bool,
        validity: Option<databend_common_column::bitmap::Bitmap>,
    ) -> std::result::Result<usize, ErrorCode> {
        let (left_type, _) = DecimalDataType::from_value(&left).unwrap();
        let (right_type, _) = DecimalDataType::from_value(&right).unwrap();
        debug_assert_eq!(left_type.size(), right_type.size());

        match (left_type, right_type) {
            (DecimalDataType::Decimal64(_), DecimalDataType::Decimal64(_)) => self
                .select_type_values_cmp::<Decimal64Type>(
                    op, left, right, validity, buffers, has_false,
                ),
            (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(_)) => self
                .select_type_values_cmp::<Decimal128Type>(
                    op, left, right, validity, buffers, has_false,
                ),
            (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(_)) => self
                .select_type_values_cmp::<Decimal256Type>(
                    op, left, right, validity, buffers, has_false,
                ),

            (DecimalDataType::Decimal64(size), DecimalDataType::Decimal128(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 => self
                        .select_type_values_cmp_lr::<Decimal64Type, Decimal128As64Type>(
                            op, left, right, validity, buffers, has_false,
                        ),

                    DecimalDataKind::Decimal128 | DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal64As128Type, Decimal128Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                }
            }
            (DecimalDataType::Decimal128(size), DecimalDataType::Decimal64(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 => self
                        .select_type_values_cmp_lr::<Decimal128As64Type, Decimal64Type>(
                            op, left, right, validity, buffers, has_false,
                        ),

                    DecimalDataKind::Decimal128 | DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal128Type, Decimal64As128Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                }
            }

            (DecimalDataType::Decimal64(size), DecimalDataType::Decimal256(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 => self
                        .select_type_values_cmp_lr::<Decimal64Type, Decimal256As64Type>(
                            op, left, right, validity, buffers, has_false,
                        ),

                    DecimalDataKind::Decimal128 | DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal64As256Type, Decimal256Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                }
            }
            (DecimalDataType::Decimal256(size), DecimalDataType::Decimal64(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 => self
                        .select_type_values_cmp_lr::<Decimal256As64Type, Decimal64Type>(
                            op, left, right, validity, buffers, has_false,
                        ),

                    DecimalDataKind::Decimal128 | DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal256Type, Decimal64As256Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                }
            }

            (DecimalDataType::Decimal128(size), DecimalDataType::Decimal256(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 | DecimalDataKind::Decimal128 => self
                        .select_type_values_cmp_lr::<Decimal128Type, Decimal256As128Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                    DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal128As256Type, Decimal256Type>(
                            op, left, right, validity, buffers, has_false,
                        ),
                }
            }
            (DecimalDataType::Decimal256(size), DecimalDataType::Decimal128(_)) => {
                match size.data_kind() {
                    DecimalDataKind::Decimal64 | DecimalDataKind::Decimal128 => self
                        .select_type_values_cmp_lr::<Decimal256As128Type, Decimal128Type>(
                        op, left, right, validity, buffers, has_false,
                    ),
                    DecimalDataKind::Decimal256 => self
                        .select_type_values_cmp_lr::<Decimal256Type, Decimal128As256Type>(
                            op, left, right, validity, buffers, has_false,
                        ),
                }
            }
        }
    }

    // Select indices by single `Value`.
    pub(super) fn select_value(
        &self,
        value: Value<AnyType>,
        data_type: &DataType,
        buffers: SelectionBuffers,
        has_false: bool,
    ) -> Result<usize> {
        debug_assert!(
            matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean))
        );

        let count = match FilterHelpers::decode_predicate(value) {
            Value::Scalar(scalar) => self.select_boolean_scalar_adapt(scalar, buffers, has_false),
            Value::Column(bitmap) => self.select_boolean_column_adapt(bitmap, buffers, has_false),
        };
        Ok(count)
    }
}
