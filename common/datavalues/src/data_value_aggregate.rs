// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataValue;
use crate::DataValueAggregateOperator;

impl DataValue {
    #[inline]
    pub fn agg(
        op: DataValueAggregateOperator,
        left: DataValue,
        right: DataValue,
    ) -> Result<DataValue> {
        match (&left, &right) {
            (DataValue::Null, _) => Result::Ok(right),
            (_, DataValue::Null) => Result::Ok(left),
            (DataValue::Int8(lhs), DataValue::Int8(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Int8, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Int8, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Int8, i8),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Int16(lhs), DataValue::Int16(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Int16, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Int16, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Int16, i16),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Int32(lhs), DataValue::Int32(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Int32, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Int32, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Int32, i32),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Int64(lhs), DataValue::Int64(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Int64, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Int64, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Int64, i64),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::UInt8(lhs), DataValue::UInt8(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, UInt8, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, UInt8, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, UInt8, u8),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::UInt16(lhs), DataValue::UInt16(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, UInt16, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, UInt16, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, UInt16, u16),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::UInt32(lhs), DataValue::UInt32(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, UInt32, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, UInt32, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, UInt32, u32),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::UInt64(lhs), DataValue::UInt64(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, UInt64, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, UInt64, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, UInt64, u64),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Float32(lhs), DataValue::Float32(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Float32, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Float32, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Float32, f32),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Float64(lhs), DataValue::Float64(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max!(lhs, rhs, Float64, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max!(lhs, rhs, Float64, max),
                DataValueAggregateOperator::Sum => typed_data_value_add!(lhs, rhs, Float64, f64),
                DataValueAggregateOperator::Count => Result::Ok(DataValue::UInt64(Some(1))),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            (DataValue::Utf8(lhs), DataValue::Utf8(rhs)) => match op {
                DataValueAggregateOperator::Min => typed_data_value_min_max_string!(lhs, rhs, Utf8, min),
                DataValueAggregateOperator::Max => typed_data_value_min_max_string!(lhs, rhs, Utf8, max),
                _ => {
                    Result::Err(ErrorCode::BadDataValueType(
                        format!(
                            "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                            op,
                            left.data_type(),
                            right.data_type()
                        )
                    ))
                }
            },
            _ => {
                Result::Err(ErrorCode::BadDataValueType(
                    format!(
                        "DataValue Error: Unsupported data_value_{} for data type: left:{:?}, right:{:?}",
                        op,
                        left.data_type(),
                        right.data_type()
                    )
                ))
            }
        }
    }
}
