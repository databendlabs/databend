// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::{FuseQueryError, FuseQueryResult};
use arrow::datatypes;

pub type DataType = datatypes::DataType;

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

// TODO: modify this, because this function is not right, eg: UInt8 * UInt8 should be UInt16
pub fn numerical_coercion(
    op: &str,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> FuseQueryResult<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return Err(FuseQueryError::Internal(format!(
            "Unsupported ({:?}) {} ({:?})",
            lhs_type, op, rhs_type,
        )));
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Ok(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    Ok(match (lhs_type, rhs_type) {
        (Float64, _) => Float64,
        (_, Float64) => Float64,

        (_, Float32) => (Float32),
        (Float32, _) => (Float32),

        (Int64, _) => (Int64),
        (_, Int64) => (Int64),

        (Int32, _) => (Int32),
        (_, Int32) => (Int32),

        (Int16, _) => (Int16),
        (_, Int16) => (Int16),

        (Int8, _) => (Int8),
        (_, Int8) => (Int8),

        (UInt64, _) => (UInt64),
        (_, UInt64) => (UInt64),

        (UInt32, _) => (UInt32),
        (_, UInt32) => (UInt32),

        (UInt16, _) => (UInt16),
        (_, UInt16) => (UInt16),

        (UInt8, _) => (UInt8),
        (_, UInt8) => (UInt8),

        _ => {
            return Err(FuseQueryError::Internal(format!(
                "Unsupported ({:?}) {} ({:?})",
                lhs_type, op, rhs_type,
            )))
        }
    })
}

pub fn equal_coercion(
    op: &str,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> FuseQueryResult<DataType> {
    if lhs_type == rhs_type {
        return Ok(lhs_type.clone());
    }
    numerical_coercion(op, lhs_type, rhs_type)
}
