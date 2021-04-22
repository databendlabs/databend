// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_arrow::arrow;

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

#[inline]
pub fn data_array_arithmetic_op(
    op: DataValueArithmeticOperator,
    left: &DataColumnarValue,
    right: &DataColumnarValue
) -> Result<DataArrayRef> {
    let (left_array, right_array) = match (left, right) {
        (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
            (left_array.clone(), right_array.clone())
        }
        (DataColumnarValue::Array(array), DataColumnarValue::Scalar(scalar)) => {
            (array.clone(), scalar.to_array_with_size(array.len())?)
        }
        (DataColumnarValue::Scalar(scalar), DataColumnarValue::Array(array)) => {
            (scalar.to_array_with_size(array.len())?, array.clone())
        }
        (DataColumnarValue::Scalar(left_scalar), DataColumnarValue::Scalar(right_scalar)) => (
            left_scalar.to_array_with_size(1)?,
            right_scalar.to_array_with_size(1)?
        )
    };

    let coercion_type = super::data_type::numerical_arithmetic_coercion(
        &op,
        &left_array.data_type(),
        &right_array.data_type()
    )?;
    let left_array = arrow::compute::cast(&left_array, &coercion_type)?;
    let right_array = arrow::compute::cast(&right_array, &coercion_type)?;
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
            arrow_primitive_array_self_defined_op!(
                &left_array,
                &right_array,
                &coercion_type,
                (|a, b| a % b)
            )
        }
    }
}
