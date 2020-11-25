// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

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
    let (larray, rarray) = match (left, right) {
        (DataColumnarValue::Array(larray), DataColumnarValue::Array(rarray)) => {
            (larray.clone(), rarray.clone())
        }
        (DataColumnarValue::Array(array), DataColumnarValue::Scalar(scalar)) => {
            (array.clone(), scalar.to_array(array.len())?)
        }
        (DataColumnarValue::Scalar(scalar), DataColumnarValue::Array(array)) => {
            (scalar.to_array(array.len())?, array.clone())
        }
        _ => {
            return Err(FuseQueryError::Unsupported(format!(
                "cannot do data_array_add, left:{:?}, right:{:?}",
                left.data_type(),
                right.data_type()
            )))
        }
    };
    match op {
        DataValueArithmeticOperator::Add => arrow_primitive_array_op!(&larray, &rarray, add),
        DataValueArithmeticOperator::Sub => arrow_primitive_array_op!(&larray, &rarray, subtract),
        DataValueArithmeticOperator::Mul => arrow_primitive_array_op!(&larray, &rarray, multiply),
        DataValueArithmeticOperator::Div => arrow_primitive_array_op!(&larray, &rarray, divide),
    }
}
