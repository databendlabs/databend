// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_array_cast;
use crate::DataArrayRef;
use crate::DataColumnarValue;
use crate::DataType;
use crate::DataValueArithmeticOperator;
use crate::Float32Array;
use crate::Float64Array;
use crate::Int16Array;
use crate::Int32Array;
use crate::Int64Array;
use crate::Int8Array;
use crate::UInt16Array;
use crate::UInt32Array;
use crate::UInt64Array;
use crate::UInt8Array;

pub struct DataArrayArithmetic;

impl DataArrayArithmetic {
    #[inline]
    pub fn data_array_arithmetic_op(
        op: DataValueArithmeticOperator,
        left: &DataColumnarValue,
        right: &DataColumnarValue,
    ) -> Result<DataArrayRef> {
        let (left_array, right_array) = match (left, right) {
            (
                DataColumnarValue::Constant(left_scalar, _),
                DataColumnarValue::Constant(right_scalar, _),
            ) => (
                left_scalar.to_array_with_size(1)?,
                right_scalar.to_array_with_size(1)?,
            ),
            _ => (left.to_array()?, right.to_array()?),
        };

        let coercion_type = super::data_type::numerical_arithmetic_coercion(
            &op,
            &left_array.data_type(),
            &right_array.data_type(),
        )?;
        let left_array = data_array_cast(&left_array, &coercion_type)?;
        let right_array = data_array_cast(&right_array, &coercion_type)?;
        match op {
            DataValueArithmeticOperator::Plus => {
                arrow_primitive_array_op!(&left_array, &right_array, &coercion_type, add)
            }
            DataValueArithmeticOperator::Minus => {
                arrow_primitive_array_op!(&left_array, &right_array, &coercion_type, subtract)
            }
            DataValueArithmeticOperator::Mul => {
                arrow_primitive_array_op!(&left_array, &right_array, &coercion_type, multiply)
            }
            DataValueArithmeticOperator::Div => {
                arrow_primitive_array_op!(&left_array, &right_array, &coercion_type, divide)
            }
            DataValueArithmeticOperator::Modulo => {
                arrow_primitive_array_op!(&left_array, &right_array, &coercion_type, modulus)
            }
        }
    }

    #[inline]
    pub fn data_array_unary_arithmetic_op(
        op: DataValueArithmeticOperator,
        value: &DataColumnarValue,
    ) -> Result<DataArrayRef> {
        match op {
            DataValueArithmeticOperator::Minus => {
                let value_array = match value {
                    DataColumnarValue::Constant(value_scalar, _) => {
                        value_scalar.to_array_with_size(1)?
                    }
                    _ => value.to_array()?,
                };

                let coercion_type =
                    super::data_type::numerical_signed_coercion(&value_array.data_type())?;
                let value_array = data_array_cast(&value_array, &coercion_type)?;
                arrow_primitive_array_negate!(&value_array, &coercion_type)
            }
            // @todo support other unary operation
            _ => Result::Err(ErrorCode::BadArguments(format!(
                "Unsupported unary operation: {:?} as argument",
                op
            ))),
        }
    }
}
