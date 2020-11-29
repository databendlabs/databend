// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datavalues;
use crate::datavalues::{DataArrayRef, DataColumnarValue, DataType, DataValueArithmeticOperator};
use crate::datavalues::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use crate::error::{FuseQueryError, FuseQueryResult};

pub fn data_array_arithmetic_op(
    op: DataValueArithmeticOperator,
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> FuseQueryResult<DataArrayRef> {
    let (left_array, right_array) = match (left, right) {
        (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
            (left_array.clone(), right_array.clone())
        }
        (DataColumnarValue::Array(array), DataColumnarValue::Scalar(scalar)) => {
            (array.clone(), scalar.to_array(array.len())?)
        }
        (DataColumnarValue::Scalar(scalar), DataColumnarValue::Array(array)) => {
            (scalar.to_array(array.len())?, array.clone())
        }
        (DataColumnarValue::Scalar(left_scalar), DataColumnarValue::Scalar(right_scalar)) => {
            (left_scalar.to_array(1)?, right_scalar.to_array(1)?)
        }
    };

    let coercion_type =
        match datavalues::numerical_coercion(&left_array.data_type(), &right_array.data_type()) {
            None => {
                return Err(FuseQueryError::Internal(format!(
                    "Unsupported ({:?}) {} ({:?})",
                    left_array.data_type(),
                    op,
                    right_array.data_type(),
                )))
            }
            Some(v) => v,
        };

    let left_array = arrow::compute::cast(&left_array, &coercion_type)?;
    let right_array = arrow::compute::cast(&right_array, &coercion_type)?;
    match op {
        DataValueArithmeticOperator::Add => {
            arrow_primitive_array_op!(&left_array, &right_array, add)
        }
        DataValueArithmeticOperator::Sub => {
            arrow_primitive_array_op!(&left_array, &right_array, subtract)
        }
        DataValueArithmeticOperator::Mul => {
            arrow_primitive_array_op!(&left_array, &right_array, multiply)
        }
        DataValueArithmeticOperator::Div => {
            arrow_primitive_array_op!(&left_array, &right_array, divide)
        }
    }
}
