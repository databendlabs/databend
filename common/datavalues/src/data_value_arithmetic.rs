// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataValue;
use crate::DataValueArithmeticOperator;
use crate::DataValueArithmeticOperator::*;

macro_rules! typed_data_value_operator {
    ($OP: expr, $LHS:expr, $RHS:expr, $SCALAR:ident, $TYPE:ident) => {{
        match $OP {
            Plus => {
                typed_data_value_add!($LHS, $RHS, $SCALAR, $TYPE)
            }
            Minus => {
                typed_data_value_sub!($LHS, $RHS, $SCALAR, $TYPE)
            }
            Mul => {
                typed_data_value_mul!($LHS, $RHS, $SCALAR, $TYPE)
            }
            Div => {
                typed_data_value_div!($LHS, $RHS, Float64, $TYPE)
            }
            Modulo => {
                typed_data_value_modulo!($LHS, $RHS, $SCALAR, $TYPE)
            }
        }
    }};
}

impl Add for &DataValue {
    type Output = Result<DataValue>;

    fn add(self, rhs: Self) -> Self::Output {
        DataValue::arithmetic(Plus, self.clone(), rhs.clone())
    }
}

impl Sub for &DataValue {
    type Output = Result<DataValue>;

    fn sub(self, rhs: Self) -> Self::Output {
        DataValue::arithmetic(Minus, self.clone(), rhs.clone())
    }
}

impl Mul for &DataValue {
    type Output = Result<DataValue>;

    fn mul(self, rhs: Self) -> Self::Output {
        DataValue::arithmetic(Mul, self.clone(), rhs.clone())
    }
}

impl Div for &DataValue {
    type Output = Result<DataValue>;

    fn div(self, rhs: Self) -> Self::Output {
        DataValue::arithmetic(Div, self.clone(), rhs.clone())
    }
}

impl Rem for &DataValue {
    type Output = Result<DataValue>;

    fn rem(self, rhs: Self) -> Self::Output {
        DataValue::arithmetic(Modulo, self.clone(), rhs.clone())
    }
}

impl Neg for &DataValue {
    type Output = Result<DataValue>;

    fn neg(self) -> Self::Output {
        if self.is_null() {
            return Ok(self.clone());
        }
        let lhs = self.to_series_with_size(1)?;
        let result = Neg::neg(&lhs)?;
        result.try_get(0)
    }
}

impl DataValue {
    #[inline]
    pub fn arithmetic(
        op: DataValueArithmeticOperator,
        left: DataValue,
        right: DataValue,
    ) -> Result<DataValue> {
        match (&left, &right) {
            (DataValue::Null, _) => Ok(right),
            (_, DataValue::Null) => Ok(left),
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

                (lhs, rhs) => Result::Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Unsupported data value operator: {:?} {} {:?}",
                    lhs.data_type(),
                    op,
                    rhs.data_type(),
                ))),
            },
        }
    }
}
