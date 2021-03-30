// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp;

use anyhow::{bail, Result};
use common_arrow::arrow::datatypes;
use common_arrow::arrow::datatypes::DataType::*;

use crate::DataValueArithmeticOperator;

pub type DataType = datatypes::DataType;

/// Determine if a DataType is signed numeric or not
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || matches!(
            dt,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        )
}

fn next_size(size: usize) -> usize {
    if size < 8_usize {
        return size * 2;
    }
    size
}

pub fn is_floating(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

pub fn is_integer(dt: &DataType) -> bool {
    is_numeric(dt) && !is_floating(dt)
}

pub fn numeric_byte_size(dt: &DataType) -> Result<usize> {
    match dt {
        DataType::Int8 | DataType::UInt8 => Ok(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Ok(2),
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => Ok(4),
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => Ok(8),
        _ => bail!("Function number_byte_size argument must be numeric types"),
    }
}

pub fn construct_numeric_type(
    is_signed: bool,
    is_floating: bool,
    byte_size: usize,
) -> Result<DataType> {
    match (is_signed, is_floating, byte_size) {
        (false, false, 1) => Ok(DataType::UInt8),
        (false, false, 2) => Ok(DataType::UInt16),
        (false, false, 4) => Ok(DataType::UInt32),
        (false, false, 8) => Ok(DataType::UInt64),
        (false, true, 1) => Ok(DataType::Float16),
        (false, true, 2) => Ok(DataType::Float16),
        (false, true, 4) => Ok(DataType::Float32),
        (false, true, 8) => Ok(DataType::Float64),
        (true, false, 1) => Ok(DataType::Int8),
        (true, false, 2) => Ok(DataType::Int16),
        (true, false, 4) => Ok(DataType::Int32),
        (true, false, 8) => Ok(DataType::Int64),
        (true, true, 1) => Ok(DataType::Float32),
        (true, true, 2) => Ok(DataType::Float32),
        (true, true, 4) => Ok(DataType::Float32),
        (true, true, 8) => Ok(DataType::Float64),

        // TODO support bigint and decimal types, now we just let's overflow
        (false, false, d) if d > 8 => Ok(DataType::Int64),
        (true, false, d) if d > 8 => Ok(DataType::UInt64),
        (_, true, d) if d > 8 => Ok(DataType::Float64),

        _ => bail!(
            "Can't construct type from is_signed: {}, is_floating: {}, byte_size: {}",
            is_signed,
            is_floating,
            byte_size
        ),
    }
}

/// Coercion rules for dictionary values (aka the type of the  dictionary itself)
fn dictionary_value_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    numerical_coercion(lhs_type, rhs_type).or_else(|_| string_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// It would likely be preferable to cast primitive values to
/// dictionaries, and thus avoid unpacking dictionary as well as doing
/// faster comparisons. However, the arrow compute kernels (e.g. eq)
/// don't have DictionaryArray support yet, so fall back to unpacking
/// the dictionaries
pub fn dictionary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => dictionary_value_coercion(lhs_value_type, rhs_value_type),
        (DataType::Dictionary(_index_type, value_type), _) => {
            dictionary_value_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            dictionary_value_coercion(lhs_type, value_type)
        }
        _ => bail!("Can't construct type from {} and {}", lhs_type, rhs_type),
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
pub fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Ok(Utf8),
        (LargeUtf8, Utf8) => Ok(LargeUtf8),
        (Utf8, LargeUtf8) => Ok(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Ok(LargeUtf8),
        _ => bail!("Can't construct type from {} and {}", lhs_type, rhs_type),
    }
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    let has_float = is_floating(lhs_type) || is_floating(rhs_type);
    let has_integer = is_integer(lhs_type) || is_integer(rhs_type);
    let has_signed = is_signed_numeric(lhs_type) || is_signed_numeric(rhs_type);
    let has_unsigned = !is_signed_numeric(lhs_type) || !is_signed_numeric(rhs_type);

    let size_of_lhs = numeric_byte_size(lhs_type)?;
    let size_of_rhs = numeric_byte_size(rhs_type)?;

    let max_size_of_unsigned_integer = cmp::max(
        if is_signed_numeric(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if is_signed_numeric(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_signed_integer = cmp::max(
        if !is_signed_numeric(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_signed_numeric(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_integer = cmp::max(
        if !is_integer(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_integer(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let max_size_of_float = cmp::max(
        if !is_floating(lhs_type) {
            0
        } else {
            size_of_lhs
        },
        if !is_floating(rhs_type) {
            0
        } else {
            size_of_rhs
        },
    );

    let should_double = (has_float && has_integer && max_size_of_integer >= max_size_of_float)
        || (has_signed
            && has_unsigned
            && max_size_of_unsigned_integer >= max_size_of_signed_integer);

    construct_numeric_type(
        has_signed,
        has_float,
        if should_double {
            cmp::max(size_of_rhs, size_of_lhs) * 2
        } else {
            cmp::max(size_of_rhs, size_of_lhs)
        },
    )
}

pub fn numerical_arithmetic_coercion(
    op: &DataValueArithmeticOperator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Result<DataType> {
    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        bail!(format!(
            "DataValue Error: Unsupported ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        ));
    };

    let has_signed = is_signed_numeric(lhs_type) || is_signed_numeric(rhs_type);
    let has_float = is_floating(lhs_type) || is_floating(rhs_type);
    let max_size = cmp::max(numeric_byte_size(lhs_type)?, numeric_byte_size(rhs_type)?);

    match op {
        DataValueArithmeticOperator::Plus | DataValueArithmeticOperator::Mul => {
            construct_numeric_type(has_signed, has_float, next_size(max_size))
        }
        DataValueArithmeticOperator::Minus => {
            construct_numeric_type(true, has_float, next_size(max_size))
        }
        DataValueArithmeticOperator::Div => Ok(Float64),
        DataValueArithmeticOperator::Modulo => {
            // https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/DivisionUtils.h#L113-L117
            let mut bytes_size = numeric_byte_size(rhs_type)?;
            if has_signed {
                bytes_size = next_size(bytes_size);
            }
            let type0 = construct_numeric_type(has_signed, false, bytes_size)?;
            if has_float {
                Ok(Float64)
            } else {
                Ok(type0)
            }
        }
    }
}

// coercion rules for equality operations. This is a superset of all numerical coercion rules.
pub fn equal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Ok(lhs_type.clone());
    }

    numerical_coercion(lhs_type, rhs_type).or_else(|_| dictionary_coercion(lhs_type, rhs_type))
}

// coercion rules that assume an ordered set, such as "less than".
// These are the union of all numerical coercion rules and all string coercion rules
#[allow(dead_code)]
pub fn order_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Result<DataType> {
    if lhs_type == rhs_type {
        // same type => all good
        return Ok(lhs_type.clone());
    }

    numerical_coercion(lhs_type, rhs_type)
        .or_else(|_| string_coercion(lhs_type, rhs_type))
        .or_else(|_| dictionary_coercion(lhs_type, rhs_type))
}
