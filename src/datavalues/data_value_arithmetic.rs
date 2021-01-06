// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::{DataValue, DataValueArithmeticOperator};
use crate::error::{FuseQueryError, FuseQueryResult};

macro_rules! typed_data_value_operator {
    ($OP: expr, $LHS:expr, $RHS:expr, $SCALAR:ident, $TYPE:ident) => {{
        match $OP {
            DataValueArithmeticOperator::Add => {
                typed_data_value_add!($LHS, $RHS, $SCALAR, $TYPE)
            }
            DataValueArithmeticOperator::Sub => {
                typed_data_value_sub!($LHS, $RHS, $SCALAR, $TYPE)
            }
            DataValueArithmeticOperator::Mul => {
                typed_data_value_mul!($LHS, $RHS, $SCALAR, $TYPE)
            }
            DataValueArithmeticOperator::Div => {
                typed_data_value_div!($LHS, $RHS, $SCALAR, $TYPE)
            }
        }
    }};
}

pub fn data_value_arithmetic_op(
    op: DataValueArithmeticOperator,
    left: DataValue,
    right: DataValue,
) -> FuseQueryResult<DataValue> {
    Ok(match (&left, &right) {
        (DataValue::Null, _) => right,
        (_, DataValue::Null) => left,
        _ => match (&left, &right) {
            // Float.
            (DataValue::Float64(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float64(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }

            // Float32.
            (DataValue::Float32(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Float32(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Float32(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }

            // UInt64.
            (DataValue::UInt64(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::UInt64(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::UInt64(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt64(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }

            // Int64.
            (DataValue::Int64(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Int64(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Int64(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::Int64(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int64(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }

            // UInt32.
            (DataValue::UInt32(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::UInt32(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::UInt32(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt32(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt32(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt32(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt32(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::UInt32(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt32(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt32(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }

            // Int32.
            (DataValue::Int32(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Int32(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Int32(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::Int32(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::Int32(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int32(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int32(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int32(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int32(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int32(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }

            // UInt16.
            (DataValue::UInt16(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::UInt16(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::UInt16(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt16(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt16(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::UInt16(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::UInt16(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::UInt16(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::UInt16(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::UInt16(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }

            // Int16.
            (DataValue::Int16(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Int16(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Int16(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::Int16(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::Int16(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::Int16(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int16, i16)
            }
            (DataValue::Int16(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int16(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int16(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int16, i16)
            }
            (DataValue::Int16(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int16, i16)
            }

            // UInt8.
            (DataValue::UInt8(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::UInt8(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::UInt8(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::UInt8(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::UInt8(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::UInt8(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt8, u8)
            }
            (DataValue::UInt8(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::UInt8(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::UInt8(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int16, i16)
            }
            (DataValue::UInt8(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt8, u8)
            }

            // Int8.
            (DataValue::Int8(lhs), DataValue::Float64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float64, f64)
            }
            (DataValue::Int8(lhs), DataValue::Float32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Float32, f32)
            }
            (DataValue::Int8(lhs), DataValue::UInt64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt64, u64)
            }
            (DataValue::Int8(lhs), DataValue::UInt32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt32, u32)
            }
            (DataValue::Int8(lhs), DataValue::UInt16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt16, u16)
            }
            (DataValue::Int8(lhs), DataValue::UInt8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, UInt8, u8)
            }
            (DataValue::Int8(lhs), DataValue::Int64(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int64, i64)
            }
            (DataValue::Int8(lhs), DataValue::Int32(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int32, i32)
            }
            (DataValue::Int8(lhs), DataValue::Int16(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int16, i16)
            }
            (DataValue::Int8(lhs), DataValue::Int8(rhs)) => {
                typed_data_value_operator!(op, lhs, rhs, Int8, i8)
            }

            (lhs, rhs) => {
                return Err(FuseQueryError::Internal(format!(
                    "Unsupported data value operator: {:?} {} {:?}",
                    lhs.data_type(),
                    op,
                    rhs.data_type(),
                )));
            }
        },
    })
}
