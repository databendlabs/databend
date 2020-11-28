// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};

pub fn data_value_add(left: DataValue, right: DataValue) -> FuseQueryResult<DataValue> {
    Ok(match (&left, &right) {
        (DataValue::Null, _) => right,
        (_, DataValue::Null) => left,
        (DataValue::Int8(lhs), DataValue::Int8(rhs)) => typed_data_value_add!(lhs, rhs, Int8, i8),
        (DataValue::Int16(lhs), DataValue::Int16(rhs)) => {
            typed_data_value_add!(lhs, rhs, Int16, i16)
        }
        (DataValue::Int32(lhs), DataValue::Int32(rhs)) => {
            typed_data_value_add!(lhs, rhs, Int32, i32)
        }
        (DataValue::Int64(lhs), DataValue::Int64(rhs)) => {
            typed_data_value_add!(lhs, rhs, Int64, i64)
        }
        (DataValue::UInt8(lhs), DataValue::UInt8(rhs)) => {
            typed_data_value_add!(lhs, rhs, UInt8, u8)
        }
        (DataValue::UInt16(lhs), DataValue::UInt16(rhs)) => {
            typed_data_value_add!(lhs, rhs, UInt16, u16)
        }
        (DataValue::UInt32(lhs), DataValue::UInt32(rhs)) => {
            typed_data_value_add!(lhs, rhs, UInt32, u32)
        }
        (DataValue::UInt64(lhs), DataValue::UInt64(rhs)) => {
            typed_data_value_add!(lhs, rhs, UInt64, u64)
        }
        (DataValue::Float32(lhs), DataValue::Float32(rhs)) => {
            typed_data_value_add!(lhs, rhs, Float32, f32)
        }
        (DataValue::Float64(lhs), DataValue::Float64(rhs)) => {
            typed_data_value_add!(lhs, rhs, Float64, f64)
        }
        _ => {
            return Err(FuseQueryError::Internal(format!(
                "Unsupported data_value_add for data type: left:{:?}, right:{:?}",
                left.data_type(),
                right.data_type()
            )))
        }
    })
}
