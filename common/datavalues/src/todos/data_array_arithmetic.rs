// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_array_cast;
use crate::DataColumn;
use crate::DataType;
use crate::DataValue;
use crate::DataValueArithmeticOperator;
use crate::Float32Array;
use crate::Float64Array;
use crate::IGetDataType;
use crate::Int16Array;
use crate::Int32Array;
use crate::Int64Array;
use crate::Int8Array;
use crate::Series;
use crate::UInt16Array;
use crate::UInt32Array;
use crate::UInt64Array;
use crate::UInt8Array;

pub struct DataArrayArithmetic;

pub fn numerical_arithmetic_coercion(
    op: &DataValueArithmeticOperator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Result<DataType> {
    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )));
    };

    let has_signed = is_signed_numeric(lhs_type) || is_signed_numeric(rhs_type);
    let has_float = is_floating(lhs_type) || is_floating(rhs_type);
    let max_size = cmp::max(numeric_byte_size(lhs_type)?, numeric_byte_size(rhs_type)?);

    match op {
        DataValueArithmeticOperator::Plus
        | DataValueArithmeticOperator::Mul
        | DataValueArithmeticOperator::Modulo => {
            construct_numeric_type(has_signed, has_float, next_size(max_size))
        }
        DataValueArithmeticOperator::Minus => {
            construct_numeric_type(true, has_float, next_size(max_size))
        }
        DataValueArithmeticOperator::Div => Ok(DataType::Float64),
    }
}

impl DataArrayArithmetic {
    #[inline]
    pub fn data_array_arithmetic_op(
        op: DataValueArithmeticOperator,
        left: &DataColumn,
        right: &DataColumn,
    ) -> Result<DataColumn> {
        match right {
            DataColumn::Array(right_array) => {
                let left_array = left.to_array()?;
                Ok(DataColumn::Array(Self::array_array_arithmetic_op(
                    op,
                    &left_array,
                    &right_array,
                )?))
            }
            DataColumn::Constant(right_value, _) => {
                let mut all_const = false;
                let left_array = match left {
                    DataColumn::Array(array) => array.clone(),
                    DataColumn::Constant(scalar, _) => {
                        all_const = true;
                        scalar.to_series_with_size(1)?
                    }
                };
                let result = Self::array_scalar_arithmetic_op(op, &left_array, right_value)?;
                if all_const {
                    let scalar = DataValue::from_array(&result, 0)?;
                    Ok(DataColumn::Constant(scalar, left.len()))
                } else {
                    Ok(DataColumn::Array(result))
                }
            }
        }
    }

    #[inline]
    fn array_array_arithmetic_op(
        op: DataValueArithmeticOperator,
        left_array: &Series,
        right_array: &Series,
    ) -> Result<Series> {
        let coercion_type = super::data_type_coercion::numerical_arithmetic_coercion(
            &op,
            &left_array.get_data_type(),
            &right_array.get_data_type(),
        )?;

        let left_array = data_array_cast(left_array, &coercion_type)?;
        let right_array = data_array_cast(right_array, &coercion_type)?;

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
    fn array_scalar_arithmetic_op(
        op: DataValueArithmeticOperator,
        left: &Series,
        right_value: &DataValue,
    ) -> Result<Series> {
        let coercion_type = super::data_type_coercion::numerical_arithmetic_coercion(
            &op,
            &left.get_data_type(),
            &right_value.data_type(),
        )?;

        let left_array = data_array_cast(left, &coercion_type)?;
        let casted_right_value = right_value.cast(&coercion_type)?;

        match op {
            DataValueArithmeticOperator::Div => {
                arrow_primitive_array_scalar_op!(
                    left_array,
                    casted_right_value,
                    &coercion_type,
                    divide
                )
            }
            DataValueArithmeticOperator::Modulo => {
                arrow_primitive_array_scalar_op!(
                    left_array,
                    casted_right_value,
                    &coercion_type,
                    modulus
                )
            }
            _ => {
                let right_array = right_value.to_series_with_size(left.len())?;
                Self::array_array_arithmetic_op(op, left, &right_array)
            }
        }
    }

    #[inline]
    pub fn data_array_unary_arithmetic_op(
        op: DataValueArithmeticOperator,
        value: &DataColumn,
    ) -> Result<Series> {
        match op {
            DataValueArithmeticOperator::Minus => {
                let value_array = match value {
                    DataColumn::Constant(value_scalar, _) => value_scalar.to_series_with_size(1)?,
                    _ => value.to_array()?,
                };

                let coercion_type = super::data_type_coercion::numerical_signed_coercion(
                    &value_array.get_data_type(),
                )?;
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
